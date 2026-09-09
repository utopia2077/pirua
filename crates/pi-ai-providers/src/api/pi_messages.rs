//! The pinned pi-messages POST/SSE protocol.
//!
//! Raw error bodies are classified transiently, then replaced by safe text that
//! preserves core retry/overflow observations. Caught failures intentionally
//! discard partial content, unlike server-supplied terminal error events.

use std::collections::BTreeMap;
use std::fmt;

use pi_ai_core::{
    ApiAdapter, ApiId, AssistantContent, AssistantMessage, AssistantMessageDiagnostic,
    AssistantMessageEvent, AssistantRole, AssistantStream, AssistantStreamInit,
    AssistantStreamProducer, BoxFuture, CacheRetention, CapabilityError, DeferredCancelRequest,
    DeferredFetchRequest, DoneReason, ErrorReason, JsonObject, JsonValue, ModelsError,
    ModelsErrorCode, REQUEST_CANCELLED_MESSAGE, RequestEndpoint, ResponseId, SafeSigned,
    SafeUnsigned, SimpleStreamRequest, StopReason, StreamOptions, StreamSetupFailure, TextContent,
    TextContentType, TextStreamRequest, ThinkingContent, ThinkingContentType, ThinkingLevel,
    ToolCall, ToolCallId, ToolCallType, ToolChoice, ToolName, Usage, UsageCost,
    create_assistant_message_event_stream, is_context_overflow, is_retryable_assistant_error,
    parse_streaming_json,
};
use pi_ai_transport::{
    RetryProfile, SseDecoder, SseProfile, TimeoutProfile, TransportError, TransportErrorKind,
    read_body_limited,
};
use serde::{Deserialize, Serialize};
use url::Url;

use super::runtime::{AdapterResponseHead, AdapterServices, cancellable};
use super::wire_json::project_wire_safe_unsigned;

fn wire_unsigned<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<SafeUnsigned, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    project_wire_safe_unsigned(&value).map_err(serde::de::Error::custom)
}

fn optional_wire_unsigned<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<SafeUnsigned>, D::Error> {
    // Only omission uses the field's default. A present null is not a number.
    wire_unsigned(deserializer).map(Some)
}

fn wire_signed<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<SafeSigned, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    let number = value
        .as_f64()
        .filter(|number| {
            number.is_finite()
                && (SafeSigned::MIN.get() as f64..=SafeSigned::MAX.get() as f64).contains(number)
                && number.fract() == 0.0
        })
        .ok_or_else(|| {
            serde::de::Error::custom("wire number must be a finite safe signed integer")
        })?;
    SafeSigned::new(number as i64).map_err(serde::de::Error::custom)
}

// This wire-only projection changes the seven count fields by numeric value.
// Remote derive keeps core Usage and its durable deserializer untouched while
// retaining missing/duplicate/unknown/null checks, including nested cost fields.
#[derive(Deserialize)]
#[serde(remote = "Usage", rename_all = "camelCase", deny_unknown_fields)]
struct PiMessagesWireUsage {
    #[serde(deserialize_with = "wire_unsigned")]
    input: SafeUnsigned,
    #[serde(deserialize_with = "wire_unsigned")]
    output: SafeUnsigned,
    #[serde(deserialize_with = "wire_unsigned")]
    cache_read: SafeUnsigned,
    #[serde(deserialize_with = "wire_unsigned")]
    cache_write: SafeUnsigned,
    #[serde(default, deserialize_with = "optional_wire_unsigned")]
    cache_write_1h: Option<SafeUnsigned>,
    #[serde(default, deserialize_with = "optional_wire_unsigned")]
    reasoning: Option<SafeUnsigned>,
    #[serde(deserialize_with = "wire_unsigned")]
    total_tokens: SafeUnsigned,
    cost: UsageCost,
}

fn optional_non_null<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)?
        .map(Some)
        .ok_or_else(|| serde::de::Error::custom("expected a non-null optional value"))
}

/// Family-specific raw options; neutral fields remain on `TextStreamRequest`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PiMessagesOptions {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub reasoning: Option<ThinkingLevel>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub tool_choice: Option<PiMessagesToolChoice>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub debug: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PiMessagesToolChoiceMode {
    Auto,
    None,
    Required,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PiMessagesFunctionType {
    Function,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PiMessagesNamedFunction {
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum PiMessagesToolChoice {
    Mode(PiMessagesToolChoiceMode),
    Function {
        #[serde(rename = "type")]
        choice_type: PiMessagesFunctionType,
        function: PiMessagesNamedFunction,
    },
}

/// Reviewed semantic metadata about a gateway's message rewrite.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PiMessagesRewriteImpact {
    pub policy_id: String,
    #[serde(deserialize_with = "wire_unsigned")]
    pub policy_version: SafeUnsigned,
    pub changed: bool,
    #[serde(deserialize_with = "wire_signed")]
    pub token_count_change: SafeSigned,
    #[serde(deserialize_with = "wire_signed")]
    pub message_count_change: SafeSigned,
    pub system_prompt_changed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PiMessagesDoneReason {
    #[serde(rename = "stop")]
    Stop,
    #[serde(rename = "length")]
    Length,
    #[serde(rename = "toolUse")]
    ToolUse,
}

/// The compact backend event shape, distinct from core assistant stream events.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all_fields = "camelCase", deny_unknown_fields)]
pub enum PiMessagesEvent {
    #[serde(rename = "start")]
    Start {},
    #[serde(rename = "text_start")]
    TextStart {
        #[serde(deserialize_with = "wire_unsigned")]
        content_index: SafeUnsigned,
    },
    #[serde(rename = "text_delta")]
    TextDelta {
        #[serde(deserialize_with = "wire_unsigned")]
        content_index: SafeUnsigned,
        delta: String,
    },
    #[serde(rename = "text_end")]
    TextEnd {
        #[serde(deserialize_with = "wire_unsigned")]
        content_index: SafeUnsigned,
        content: String,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "optional_non_null"
        )]
        content_signature: Option<String>,
    },
    #[serde(rename = "thinking_start")]
    ThinkingStart {
        #[serde(deserialize_with = "wire_unsigned")]
        content_index: SafeUnsigned,
    },
    #[serde(rename = "thinking_delta")]
    ThinkingDelta {
        #[serde(deserialize_with = "wire_unsigned")]
        content_index: SafeUnsigned,
        delta: String,
    },
    #[serde(rename = "thinking_end")]
    ThinkingEnd {
        #[serde(deserialize_with = "wire_unsigned")]
        content_index: SafeUnsigned,
        content: String,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "optional_non_null"
        )]
        content_signature: Option<String>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "optional_non_null"
        )]
        redacted: Option<bool>,
    },
    #[serde(rename = "toolcall_start")]
    ToolCallStart {
        #[serde(deserialize_with = "wire_unsigned")]
        content_index: SafeUnsigned,
        id: ToolCallId,
        tool_name: ToolName,
    },
    #[serde(rename = "toolcall_delta")]
    ToolCallDelta {
        #[serde(deserialize_with = "wire_unsigned")]
        content_index: SafeUnsigned,
        delta: String,
    },
    #[serde(rename = "toolcall_end")]
    ToolCallEnd {
        #[serde(deserialize_with = "wire_unsigned")]
        content_index: SafeUnsigned,
        tool_call: ToolCall,
    },
    #[serde(rename = "done")]
    Done {
        reason: PiMessagesDoneReason,
        #[serde(deserialize_with = "PiMessagesWireUsage::deserialize")]
        usage: Usage,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "optional_non_null"
        )]
        response_id: Option<ResponseId>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "optional_non_null"
        )]
        rewrite: Option<PiMessagesRewriteImpact>,
    },
    #[serde(rename = "error")]
    Error {
        reason: ErrorReason,
        #[serde(deserialize_with = "PiMessagesWireUsage::deserialize")]
        usage: Usage,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "optional_non_null"
        )]
        error_message: Option<String>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "optional_non_null"
        )]
        response_id: Option<ResponseId>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "optional_non_null"
        )]
        rewrite: Option<PiMessagesRewriteImpact>,
    },
}

impl fmt::Debug for PiMessagesEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PiMessagesEvent([REDACTED])")
    }
}

/// Transient HTTP error projection, retaining safe control information only.
pub struct PiMessagesResponseError {
    pub status: u16,
    code: Option<String>,
    pub context_overflow: bool,
    pub retryable: bool,
}

impl PiMessagesResponseError {
    /// Provider code for explicit trusted inspection; never emitted in diagnostics.
    pub fn code_for_inspection(&self) -> Option<&str> {
        self.code.as_deref()
    }

    /// Classifies raw provider detail before discarding its body and message.
    /// The transport's Latin-1 byte-string projection of the HTTP/1 phrase (or
    /// HTTP/2 empty string) is decoded like Node Fetch before classification.
    /// Out-of-range raw scalars reject the port value as a protocol failure.
    /// The decoded phrase is never retained or rendered in diagnostics.
    pub fn from_response(
        status: u16,
        status_text: &str,
        body: &[u8],
        template: &AssistantMessage,
    ) -> Result<Self, TransportError> {
        let status_text = decode_fetch_status_text(status_text)?;
        let decoded = String::from_utf8_lossy(body);
        let raw = decoded.strip_prefix('\u{feff}').unwrap_or(&decoded);
        let parsed = serde_json::from_str::<serde_json::Value>(raw).ok();
        let error = parsed
            .as_ref()
            .and_then(|v| v.get("error"))
            .and_then(|v| v.as_object());
        let message = error
            .and_then(|e| e.get("message"))
            .and_then(|v| v.as_str())
            .unwrap_or(raw);
        let code = error
            .and_then(|e| e.get("code"))
            .and_then(|v| v.as_str())
            .map(str::to_owned);
        let mut probe = template.clone();
        probe.stop_reason = StopReason::Error;
        probe.error_message = Some(format!(
            "{status} {status_text}: {message}{}",
            code.as_ref().map(|c| format!(" ({c})")).unwrap_or_default()
        ));
        Ok(Self {
            status,
            code,
            context_overflow: is_context_overflow(&probe, None),
            retryable: is_retryable_assistant_error(&probe),
        })
    }

    pub fn diagnostic_details(&self) -> JsonObject {
        object(
            serde_json::json!({ "version": 1, "status": self.status, "contextOverflow": self.context_overflow, "retryable": self.retryable }),
        )
    }
}

fn decode_fetch_status_text(raw: &str) -> Result<String, TransportError> {
    let bytes = raw
        .chars()
        .map(|character| {
            u8::try_from(u32::from(character))
                .map_err(|_| TransportError::new(TransportErrorKind::Protocol))
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Actual Node Fetch preserves BOMs in statusText, unlike Response.text().
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

#[cfg(test)]
mod status_text_tests {
    use super::decode_fetch_status_text;
    use pi_ai_transport::TransportErrorKind;

    #[test]
    fn recovered_reason_bytes_match_actual_node_fetch_and_preserve_boms() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../conformance/provider-traces/pi-messages/status-text-decoding.json"
        ))
        .unwrap();
        assert_eq!(fixture["cases"].as_array().unwrap().len(), 10);
        for case in fixture["cases"].as_array().unwrap() {
            let bytes: Vec<u8> = serde_json::from_value(case["bytes"].clone()).unwrap();
            let raw: String = bytes.into_iter().map(char::from).collect();
            assert_eq!(
                decode_fetch_status_text(&raw).unwrap(),
                case["statusText"].as_str().unwrap(),
                "{}",
                case["id"]
            );
        }
    }

    #[test]
    fn already_decoded_out_of_domain_scalars_are_not_silently_truncated() {
        assert_eq!(decode_fetch_status_text("").unwrap(), "");
        for invalid in [
            "\u{100}",
            "server\u{fffd}error",
            "\u{feff}prompt is too long",
            "😀",
        ] {
            let error = decode_fetch_status_text(invalid).unwrap_err();
            assert_eq!(error.kind(), TransportErrorKind::Protocol);
            assert!(!format!("{error} {error:?}").contains(invalid));
        }
    }
}

impl fmt::Debug for PiMessagesResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PiMessagesResponseError")
            .field("status", &self.status)
            .field("context_overflow", &self.context_overflow)
            .field("retryable", &self.retryable)
            .finish()
    }
}
impl fmt::Display for PiMessagesResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "pi-messages HTTP status {}: {}",
            self.status,
            classified_message(self.context_overflow, self.retryable)
        )
    }
}
impl std::error::Error for PiMessagesResponseError {}

/// Additional bounded reducer state, configured by the trusted host.
#[derive(Clone, Copy, Debug)]
pub struct PiMessagesLimits {
    pub max_content_blocks: usize,
    pub max_stream_bytes: usize,
    pub max_tool_input_bytes: usize,
}
impl Default for PiMessagesLimits {
    fn default() -> Self {
        Self {
            max_content_blocks: 4096,
            max_stream_bytes: 16 * 1024 * 1024,
            max_tool_input_bytes: 262_144,
        }
    }
}

/// Canonical pi-messages adapter; construction does not register providers or do I/O.
#[derive(Clone, Debug)]
pub struct PiMessagesApi {
    api: ApiId,
    pub services: AdapterServices,
    pub defaults: PiMessagesOptions,
    pub limits: PiMessagesLimits,
}

impl PiMessagesApi {
    pub fn new(services: AdapterServices) -> Self {
        Self {
            api: ApiId::new("pi-messages").expect("constant API"),
            services,
            defaults: PiMessagesOptions::default(),
            limits: PiMessagesLimits::default(),
        }
    }

    /// Per-invocation family options replace defaults; request-owned neutral fields
    /// are authoritative. Setup/spawn/transport failures are in-band terminals.
    pub fn stream_with_options(
        &self,
        request: TextStreamRequest,
        options: PiMessagesOptions,
    ) -> AssistantStream {
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: request.resolved.model.api.clone(),
            provider: request.resolved.model.provider.clone(),
            model: request.resolved.model.id.clone(),
            clock: request.options.runtime.clock.clone(),
            cancellation: request.options.runtime.cancellation.clone(),
        });
        if request.resolved.model.api != self.api {
            publish_failure(&request, &producer, Failure::ApiMismatch);
            return stream;
        }
        let services = self.services.clone();
        let limits = self.limits;
        let partial = empty_message(&request);
        let spawner = request.options.runtime.spawner.clone();
        let worker = producer.clone();
        let task = Box::pin(async move {
            if let Err(error) = run(&request, &options, &services, limits, &worker, partial).await {
                publish_failure(&request, &worker, error);
            }
        });
        if spawner.spawn(task).is_err() {
            let _ = producer.fail_setup(StreamSetupFailure::Spawn);
        }
        stream
    }
}

impl ApiAdapter for PiMessagesApi {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        self.stream_with_options(request, self.defaults.clone())
    }
    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        let options = PiMessagesOptions {
            reasoning: request.options.wire.reasoning,
            tool_choice: request.options.wire.tool_choice.map(|choice| {
                PiMessagesToolChoice::Mode(match choice {
                    ToolChoice::Auto => PiMessagesToolChoiceMode::Auto,
                    ToolChoice::None => PiMessagesToolChoiceMode::None,
                })
            }),
            debug: self.defaults.debug,
        };
        self.stream_with_options(
            TextStreamRequest {
                resolved: request.resolved,
                context: request.context,
                auth: request.auth,
                options: StreamOptions {
                    wire: request.options.wire.stream,
                    runtime: request.options.runtime,
                },
            },
            options,
        )
    }
    fn fetch_deferred(
        &self,
        _request: DeferredFetchRequest,
    ) -> Result<AssistantStream, CapabilityError> {
        Err(CapabilityError::Unsupported)
    }
    fn cancel_deferred<'a>(
        &'a self,
        _request: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async {
            Err(ModelsError::with_source(
                ModelsErrorCode::Stream,
                CapabilityError::Unsupported,
            ))
        })
    }
}

enum Failure {
    ApiMismatch,
    Authentication,
    PayloadMiddleware,
    ResponseMiddleware,
    Protocol,
    MissingTerminal,
    NoBody,
    Limit,
    Transport(TransportErrorKind),
    Http(PiMessagesResponseError),
}
impl From<TransportError> for Failure {
    fn from(value: TransportError) -> Self {
        Self::Transport(value.kind())
    }
}

fn timestamp(request: &TextStreamRequest) -> SafeUnsigned {
    SafeUnsigned::new(request.options.runtime.clock.now_unix_ms()).unwrap_or(SafeUnsigned::MAX)
}

fn empty_message(request: &TextStreamRequest) -> AssistantMessage {
    AssistantMessage {
        role: AssistantRole::Assistant,
        content: Vec::new(),
        api: request.resolved.model.api.clone(),
        provider: request.resolved.model.provider.clone(),
        model: request.resolved.model.id.clone(),
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: Usage::zero(),
        stop_reason: StopReason::Pending,
        deferred: None,
        error_message: None,
        raw_stop_reason: None,
        end_turn: None,
        timestamp: timestamp(request),
    }
}

fn classified_message(overflow: bool, retryable: bool) -> &'static str {
    match (overflow, retryable) {
        (true, true) => "prompt is too long; server error",
        (true, false) => "prompt is too long",
        (false, true) => "server error",
        (false, false) => "provider request failed",
    }
}

fn object(value: serde_json::Value) -> JsonObject {
    serde_json::from_value(value).expect("fixed safe diagnostic object")
}

fn publish_failure(
    request: &TextStreamRequest,
    producer: &AssistantStreamProducer,
    failure: Failure,
) {
    let mut message = empty_message(request);
    let aborted = request.options.runtime.cancellation.is_cancelled();
    message.stop_reason = if aborted {
        StopReason::Aborted
    } else {
        StopReason::Error
    };
    let (kind, detail) = match &failure {
        Failure::ApiMismatch => ("api_mismatch", "pi-messages API mismatch"),
        Failure::Authentication => ("authentication", "No API key provided for pi-messages"),
        Failure::PayloadMiddleware => (
            "payload_middleware",
            "pi-messages payload middleware failed",
        ),
        Failure::ResponseMiddleware => (
            "response_middleware",
            "pi-messages response middleware failed",
        ),
        Failure::Protocol => ("protocol", "invalid pi-messages protocol event"),
        Failure::MissingTerminal => (
            "missing_terminal",
            "pi-messages stream ended without a terminal event",
        ),
        Failure::NoBody => ("no_body", "pi-messages response has no body"),
        Failure::Limit => ("limit", "pi-messages resource limit exceeded"),
        Failure::Transport(TransportErrorKind::Connection) => ("transport", "network error"),
        Failure::Transport(TransportErrorKind::Timeout) => ("transport", "request timed out"),
        Failure::Transport(_) => ("transport", "pi-messages transport failure"),
        Failure::Http(error) => (
            "http",
            classified_message(error.context_overflow, error.retryable),
        ),
    };
    message.error_message = Some(
        if aborted {
            REQUEST_CANCELLED_MESSAGE
        } else {
            detail
        }
        .to_owned(),
    );
    if !aborted {
        let (category, details) = match &failure {
            Failure::Http(error) => ("pi_messages_response_failure", error.diagnostic_details()),
            _ => (
                "pi_messages_failure",
                object(serde_json::json!({ "kind": kind })),
            ),
        };
        message.diagnostics = Some(vec![AssistantMessageDiagnostic {
            diagnostic_type: category.parse().expect("constant diagnostic type"),
            timestamp: message.timestamp,
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
        error: Box::new(message),
    });
}

async fn run(
    request: &TextStreamRequest,
    options: &PiMessagesOptions,
    services: &AdapterServices,
    limits: PiMessagesLimits,
    producer: &AssistantStreamProducer,
    partial: AssistantMessage,
) -> Result<(), Failure> {
    let runtime = &request.options.runtime;
    let api_key = request
        .auth
        .api_key
        .as_ref()
        .map(|key| key.expose_secret())
        .filter(|key| !key.is_empty())
        .ok_or(Failure::Authentication)?;
    let base = request.resolved.endpoint.expose_for_transport();
    let mut url = Url::parse(&format!("{}/messages", base.trim_end_matches('/')))
        .map_err(|_| Failure::Protocol)?;
    if options.debug == Some(true) {
        let mut found = false;
        let mut pairs: Vec<(String, String)> = url
            .query_pairs()
            .filter_map(|(key, value)| {
                if key != "debug" {
                    return Some((key.into_owned(), value.into_owned()));
                }
                if found {
                    return None;
                }
                found = true;
                Some(("debug".to_owned(), "1".to_owned()))
            })
            .collect();
        if !found {
            pairs.push(("debug".to_owned(), "1".to_owned()));
        }
        url.query_pairs_mut().clear().extend_pairs(pairs);
    }
    let wire = &request.options.wire;
    let cache = wire.cache_retention.or_else(|| {
        request
            .auth
            .env
            .as_map()
            .get("PI_CACHE_RETENTION")
            .filter(|v| !v.is_empty())
            .or_else(|| services.env.as_map().get("PI_CACHE_RETENTION"))
            .filter(|v| v.as_str() == "long")
            .map(|_| CacheRetention::Long)
    });
    let mut payload_options = serde_json::Map::new();
    for (name, value) in [
        (
            "temperature",
            wire.temperature
                .map(|v| serde_json::to_value(v).expect("finite number")),
        ),
        (
            "maxTokens",
            wire.max_tokens
                .map(|v| serde_json::to_value(v).expect("safe count")),
        ),
        (
            "reasoning",
            options
                .reasoning
                .map(|v| serde_json::to_value(v).expect("reasoning level")),
        ),
        (
            "cacheRetention",
            cache.map(|v| serde_json::to_value(v).expect("cache retention")),
        ),
        (
            "sessionId",
            wire.session_id.clone().map(serde_json::Value::String),
        ),
        (
            "toolChoice",
            options
                .tool_choice
                .as_ref()
                .map(|v| serde_json::to_value(v).expect("tool choice")),
        ),
    ] {
        if let Some(value) = value {
            payload_options.insert(name.to_owned(), value);
        }
    }
    let mut payload = super::wire_json::WirePayload::new(
        serde_json::json!({ "model": request.resolved.model.id, "context": request.context, "options": payload_options }),
    );
    if let Some(middleware) = &services.payload
        && let Some(replacement) = cancellable(
            middleware.on_payload(request, &mut payload),
            &runtime.cancellation,
        )
        .await
        .map_err(|_| Failure::Transport(TransportErrorKind::Cancelled))?
        .map_err(|_| Failure::PayloadMiddleware)?
    {
        payload = replacement;
    }
    let mut raw_headers = vec![
        ("authorization".to_owned(), format!("Bearer {api_key}")),
        ("accept".to_owned(), "text/event-stream".to_owned()),
        ("content-type".to_owned(), "application/json".to_owned()),
    ];
    // Source filters null before merging: null does not suppress a default.
    for (name, value) in request.auth.headers.iter_for_dispatch() {
        if let Some(value) = value {
            if let Some((_, old)) = raw_headers.iter_mut().find(|(key, _)| key == name) {
                *old = value.clone();
            } else {
                raw_headers.push((name.clone(), value.clone()));
            }
        }
    }
    let mut headers = pi_ai_transport::FetchHeaderRecord::new();
    for (name, value) in raw_headers {
        headers.insert(name, value);
    }
    let http_request = pi_ai_transport::FetchRequest {
        endpoint: RequestEndpoint::new_resolved(url.as_str()).map_err(|_| Failure::Protocol)?,
        method: "POST".to_owned(),
        headers: pi_ai_transport::FetchHeaders::Record(headers),
        body: pi_ai_transport::FetchBody::Text(
            super::wire_json::stringify_wire_json_without_fixed_limits(payload.as_value())
                .map_err(|_| Failure::Protocol)?,
        ),
        proxy: None,
    };
    let mut response = services
        .transport
        .send_fetch_request(
            &http_request,
            runtime,
            RetryProfile::None,
            TimeoutProfile::None,
        )
        .await?;
    if let Some(middleware) = &services.response {
        let head = AdapterResponseHead::from_http(response.status, &response.headers)?;
        cancellable(
            middleware.on_response(request, &head),
            &runtime.cancellation,
        )
        .await
        .map_err(|_| Failure::Transport(TransportErrorKind::Cancelled))?
        .map_err(|_| Failure::ResponseMiddleware)?;
    }
    if !(200..300).contains(&response.status) {
        let body = read_body_limited(
            response.body.as_mut(),
            runtime,
            services.max_error_body_bytes,
        )
        .await?;
        return Err(Failure::Http(PiMessagesResponseError::from_response(
            response.status,
            &response.status_text,
            &body,
            &empty_message(request),
        )?));
    }
    if matches!(response.status, 204 | 205) {
        return Err(Failure::NoBody);
    }
    let mut reducer = Reducer {
        partial,
        tool_json: BTreeMap::new(),
        limits,
    };
    let mut decoder = SseDecoder::new(SseProfile::PiMessages, services.sse_limits);
    let mut total = 0usize;
    let mut ended = false;
    loop {
        while let Some(frame) = decoder.next_frame()? {
            total = total.checked_add(frame.data.len()).ok_or(Failure::Limit)?;
            if total > limits.max_stream_bytes {
                return Err(Failure::Limit);
            }
            let event: PiMessagesEvent =
                serde_json::from_str(&frame.data).map_err(|_| Failure::Protocol)?;
            let event = reducer.convert(event, request)?;
            let terminal = matches!(
                event,
                AssistantMessageEvent::Done { .. } | AssistantMessageEvent::Error { .. }
            );
            producer.push(event).map_err(|_| Failure::Protocol)?;
            if terminal {
                response.body.close();
                return Ok(());
            }
        }
        if ended {
            return Err(Failure::MissingTerminal);
        }
        match response.body.next(runtime).await? {
            Some(chunk) => decoder.push(&chunk)?,
            None => {
                decoder.end_input();
                ended = true;
            }
        }
    }
}

struct Reducer {
    partial: AssistantMessage,
    tool_json: BTreeMap<usize, String>,
    limits: PiMessagesLimits,
}
impl Reducer {
    fn index(&self, value: SafeUnsigned) -> Result<usize, Failure> {
        let index = usize::try_from(value.get()).map_err(|_| Failure::Limit)?;
        if index >= self.limits.max_content_blocks {
            return Err(Failure::Limit);
        }
        Ok(index)
    }
    fn start(&mut self, index: usize, block: AssistantContent) -> Result<(), Failure> {
        if index == self.partial.content.len() {
            self.partial.content.push(block);
        } else if let Some(slot) = self.partial.content.get_mut(index) {
            *slot = block;
        } else {
            return Err(Failure::Protocol);
        }
        Ok(())
    }
    fn rewrite(
        &mut self,
        rewrite: Option<PiMessagesRewriteImpact>,
        request: &TextStreamRequest,
    ) -> Result<(), Failure> {
        if let Some(rewrite) = rewrite {
            let details = serde_json::from_value(
                serde_json::to_value(rewrite).map_err(|_| Failure::Protocol)?,
            )
            .map_err(|_| Failure::Protocol)?;
            self.partial.diagnostics.get_or_insert_with(Vec::new).push(
                AssistantMessageDiagnostic {
                    diagnostic_type: "pi_messages_rewrite"
                        .parse()
                        .expect("constant diagnostic type"),
                    timestamp: timestamp(request),
                    error: None,
                    details: Some(details),
                },
            );
        }
        Ok(())
    }
    fn convert(
        &mut self,
        event: PiMessagesEvent,
        request: &TextStreamRequest,
    ) -> Result<AssistantMessageEvent, Failure> {
        Ok(match event {
            PiMessagesEvent::Start {} => AssistantMessageEvent::Start {
                partial: Box::new(self.partial.clone()),
            },
            PiMessagesEvent::TextStart { content_index } => {
                self.start(
                    self.index(content_index)?,
                    AssistantContent::Text(TextContent {
                        content_type: TextContentType::Text,
                        text: String::new(),
                        text_signature: None,
                    }),
                )?;
                AssistantMessageEvent::TextStart {
                    content_index,
                    partial: Box::new(self.partial.clone()),
                }
            }
            PiMessagesEvent::TextDelta {
                content_index,
                delta,
            } => {
                let index = self.index(content_index)?;
                let Some(AssistantContent::Text(block)) = self.partial.content.get_mut(index)
                else {
                    return Err(Failure::Protocol);
                };
                block.text.push_str(&delta);
                AssistantMessageEvent::TextDelta {
                    content_index,
                    delta,
                    partial: Box::new(self.partial.clone()),
                }
            }
            PiMessagesEvent::TextEnd {
                content_index,
                content,
                content_signature,
            } => {
                let index = self.index(content_index)?;
                let Some(AssistantContent::Text(block)) = self.partial.content.get_mut(index)
                else {
                    return Err(Failure::Protocol);
                };
                block.text = content.clone();
                block.text_signature = content_signature;
                AssistantMessageEvent::TextEnd {
                    content_index,
                    content,
                    partial: Box::new(self.partial.clone()),
                }
            }
            PiMessagesEvent::ThinkingStart { content_index } => {
                self.start(
                    self.index(content_index)?,
                    AssistantContent::Thinking(ThinkingContent {
                        content_type: ThinkingContentType::Thinking,
                        thinking: String::new(),
                        thinking_signature: None,
                        redacted: None,
                    }),
                )?;
                AssistantMessageEvent::ThinkingStart {
                    content_index,
                    partial: Box::new(self.partial.clone()),
                }
            }
            PiMessagesEvent::ThinkingDelta {
                content_index,
                delta,
            } => {
                let index = self.index(content_index)?;
                let Some(AssistantContent::Thinking(block)) = self.partial.content.get_mut(index)
                else {
                    return Err(Failure::Protocol);
                };
                block.thinking.push_str(&delta);
                AssistantMessageEvent::ThinkingDelta {
                    content_index,
                    delta,
                    partial: Box::new(self.partial.clone()),
                }
            }
            PiMessagesEvent::ThinkingEnd {
                content_index,
                content,
                content_signature,
                redacted,
            } => {
                let index = self.index(content_index)?;
                let Some(AssistantContent::Thinking(block)) = self.partial.content.get_mut(index)
                else {
                    return Err(Failure::Protocol);
                };
                block.thinking = content.clone();
                block.thinking_signature = content_signature;
                block.redacted = redacted;
                AssistantMessageEvent::ThinkingEnd {
                    content_index,
                    content,
                    partial: Box::new(self.partial.clone()),
                }
            }
            PiMessagesEvent::ToolCallStart {
                content_index,
                id,
                tool_name,
            } => {
                let index = self.index(content_index)?;
                self.start(
                    index,
                    AssistantContent::ToolCall(ToolCall {
                        content_type: ToolCallType::ToolCall,
                        id,
                        name: tool_name,
                        arguments: JsonObject::default(),
                        thought_signature: None,
                        namespace: None,
                    }),
                )?;
                self.tool_json.insert(index, String::new());
                AssistantMessageEvent::ToolCallStart {
                    content_index,
                    partial: Box::new(self.partial.clone()),
                }
            }
            PiMessagesEvent::ToolCallDelta {
                content_index,
                delta,
            } => {
                let index = self.index(content_index)?;
                let buffer = self.tool_json.get_mut(&index).ok_or(Failure::Protocol)?;
                if delta.len()
                    > self
                        .limits
                        .max_tool_input_bytes
                        .saturating_sub(buffer.len())
                {
                    return Err(Failure::Limit);
                }
                buffer.push_str(&delta);
                let arguments = match parse_streaming_json(Some(buffer)) {
                    JsonValue::Object(value) => value,
                    _ => return Err(Failure::Protocol),
                };
                let Some(AssistantContent::ToolCall(block)) = self.partial.content.get_mut(index)
                else {
                    return Err(Failure::Protocol);
                };
                block.arguments = arguments;
                AssistantMessageEvent::ToolCallDelta {
                    content_index,
                    delta,
                    partial: Box::new(self.partial.clone()),
                }
            }
            PiMessagesEvent::ToolCallEnd {
                content_index,
                tool_call,
            } => {
                let index = self.index(content_index)?;
                let Some(AssistantContent::ToolCall(block)) = self.partial.content.get_mut(index)
                else {
                    return Err(Failure::Protocol);
                };
                // Object.assign upstream keeps optional fields omitted by the end event.
                let mut tool_call = tool_call;
                if tool_call.thought_signature.is_none() {
                    tool_call.thought_signature = block.thought_signature.clone();
                }
                if tool_call.namespace.is_none() {
                    tool_call.namespace = block.namespace.clone();
                }
                *block = tool_call.clone();
                self.tool_json.remove(&index);
                AssistantMessageEvent::ToolCallEnd {
                    content_index,
                    tool_call,
                    partial: Box::new(self.partial.clone()),
                }
            }
            PiMessagesEvent::Done {
                reason,
                usage,
                response_id,
                rewrite,
            } => {
                let (reason, stop) = match reason {
                    PiMessagesDoneReason::Stop => (DoneReason::Stop, StopReason::Stop),
                    PiMessagesDoneReason::Length => (DoneReason::Length, StopReason::Length),
                    PiMessagesDoneReason::ToolUse => (DoneReason::ToolUse, StopReason::ToolUse),
                };
                self.partial.stop_reason = stop;
                self.partial.usage = usage;
                self.partial.response_id = response_id;
                self.rewrite(rewrite, request)?;
                AssistantMessageEvent::Done {
                    reason,
                    message: Box::new(self.partial.clone()),
                }
            }
            PiMessagesEvent::Error {
                reason,
                usage,
                error_message,
                response_id,
                rewrite,
            } => {
                self.partial.stop_reason = if reason == ErrorReason::Aborted {
                    StopReason::Aborted
                } else {
                    StopReason::Error
                };
                self.partial.usage = usage;
                self.partial.response_id = response_id;
                self.partial.error_message = error_message;
                let safe = if reason == ErrorReason::Aborted {
                    REQUEST_CANCELLED_MESSAGE
                } else {
                    classified_message(
                        is_context_overflow(&self.partial, None),
                        is_retryable_assistant_error(&self.partial),
                    )
                };
                self.partial.error_message = Some(safe.to_owned());
                self.rewrite(rewrite, request)?;
                AssistantMessageEvent::Error {
                    reason,
                    error: Box::new(self.partial.clone()),
                }
            }
        })
    }
}
