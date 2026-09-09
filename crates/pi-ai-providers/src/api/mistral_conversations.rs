//! Native Mistral Chat Completions protocol at the pinned pi-mono revision.
//! Logical camelCase middleware precedes the finite HTTP wire-name projection.
//! Caller cancellation retains the latest owned partial message; timeouts are errors.

use std::{fmt, time::Duration};

use pi_ai_core::*;
use pi_ai_transport::{
    HttpHeaders, HttpRequest, RetryProfile, SseDecoder, SseProfile, TimeoutProfile, TransportError,
    TransportErrorKind, read_body_limited,
};
use serde::{Deserialize, Serialize};
use url::Url;

use super::runtime::{AdapterResponseHead, AdapterServices, cancellable};
use super::simple_options::build_base_options;
use super::wire_json::stringify_wire_json;

mod payload;
mod reducer;

fn optional_non_null<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(deserializer)?
        .map(Some)
        .ok_or_else(|| serde::de::Error::custom("expected a non-null optional value"))
}

/// Raw family fields; credentials and neutral options remain request-owned.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MistralOptions {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub tool_choice: Option<MistralToolChoice>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub prompt_mode: Option<MistralPromptMode>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub reasoning_effort: Option<MistralReasoningEffort>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MistralPromptMode {
    Reasoning,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MistralReasoningEffort {
    None,
    High,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MistralToolChoiceMode {
    Auto,
    None,
    Any,
    Required,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MistralFunctionType {
    Function,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MistralNamedFunction {
    pub name: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum MistralToolChoice {
    Mode(MistralToolChoiceMode),
    Function {
        #[serde(rename = "type")]
        choice_type: MistralFunctionType,
        function: MistralNamedFunction,
    },
}

/// Trusted resource ceilings, not provider-visible sampling controls.
#[derive(Clone, Copy, Debug)]
pub struct MistralLimits {
    pub max_content_blocks: usize,
    pub max_stream_bytes: usize,
    pub max_tool_input_bytes: usize,
}
impl Default for MistralLimits {
    fn default() -> Self {
        Self {
            max_content_blocks: 4096,
            max_stream_bytes: 16 * 1024 * 1024,
            max_tool_input_bytes: 262_144,
        }
    }
}

/// Passive construction. Hosts can supply the pinned Node OS user-agent snapshot;
/// the portable default is the declared Rust identity, without OS discovery.
#[derive(Clone)]
pub struct MistralApi {
    api: ApiId,
    pub services: AdapterServices,
    pub defaults: MistralOptions,
    pub limits: MistralLimits,
    pub user_agent: String,
}
impl fmt::Debug for MistralApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MistralApi([REDACTED])")
    }
}
impl MistralApi {
    pub fn new(services: AdapterServices) -> Self {
        Self {
            api: ApiId::new("mistral-conversations").expect("constant API"),
            services,
            defaults: MistralOptions::default(),
            limits: MistralLimits::default(),
            user_agent: "pi-ai-rust".to_owned(),
        }
    }
    pub fn stream_with_options(
        &self,
        request: TextStreamRequest,
        options: MistralOptions,
    ) -> AssistantStream {
        self.start(request, Ok((options, None)))
    }
    fn start(
        &self,
        request: TextStreamRequest,
        options: Result<(MistralOptions, Option<String>), Failure>,
    ) -> AssistantStream {
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: request.resolved.model.api.clone(),
            provider: request.resolved.model.provider.clone(),
            model: request.resolved.model.id.clone(),
            clock: request.options.runtime.clock.clone(),
            cancellation: request.options.runtime.cancellation.clone(),
        });
        let partial = empty_message(&request);
        if request.resolved.model.api != self.api {
            publish_failure(&request, &producer, partial, Failure::ApiMismatch);
            return stream;
        }
        let adapter = self.clone();
        let spawner = request.options.runtime.spawner.clone();
        let worker = producer.clone();
        if spawner
            .spawn(Box::pin(async move {
                let mut reducer = reducer::Reducer::new(partial, adapter.limits);
                let outcome = match options {
                    Ok((options, mapped_effort)) => {
                        adapter
                            .run(
                                &request,
                                &options,
                                mapped_effort.as_deref(),
                                &worker,
                                &mut reducer,
                            )
                            .await
                    }
                    Err(error) => Err(error),
                };
                if let Err(error) = outcome {
                    publish_failure(&request, &worker, reducer.partial, error);
                }
            }))
            .is_err()
        {
            let _ = producer.fail_setup(StreamSetupFailure::Spawn);
        }
        stream
    }

    async fn run(
        &self,
        request: &TextStreamRequest,
        options: &MistralOptions,
        mapped_effort: Option<&str>,
        producer: &AssistantStreamProducer,
        reducer: &mut reducer::Reducer,
    ) -> Result<(), Failure> {
        let runtime = &request.options.runtime;
        let key = request
            .auth
            .api_key
            .as_ref()
            .map(|key| key.expose_secret())
            .filter(|key| !key.is_empty())
            .ok_or(Failure::Authentication)?;
        let mut logical = payload::build(request, options)?;
        // The pinned simple mapper casts model-owned strings without narrowing.
        // Keep raw caller options closed while preserving that model projection.
        if let Some(effort) = mapped_effort.filter(|effort| !effort.is_empty()) {
            logical["reasoningEffort"] = serde_json::Value::String(effort.into());
        }
        let mut payload = super::wire_json::WirePayload::new(logical);
        if let Some(hook) = &self.services.payload
            && let Some(replacement) = cancellable(
                hook.on_payload(request, &mut payload),
                &runtime.cancellation,
            )
            .await
            .map_err(|_| Failure::Cancelled)?
            .map_err(|_| Failure::Middleware)?
        {
            payload = replacement;
        }
        let wire = payload::to_wire(payload.into_value())?;
        let mut base = Url::parse(request.resolved.endpoint.expose_for_transport())
            .map_err(|_| Failure::Protocol)?;
        base.set_path(&format!("{}/", base.path().trim_end_matches('/')));
        let url = base
            .join("v1/chat/completions")
            .map_err(|_| Failure::Protocol)?;
        let mut headers = HttpHeaders::new();
        for (name, value) in [
            ("user-agent", self.user_agent.clone()),
            ("accept", "text/event-stream".into()),
            ("authorization", format!("Bearer {key}")),
            ("content-type", "application/json".into()),
        ] {
            set_header(&mut headers, name, &value)?;
        }
        if let Some(model_headers) = &request.resolved.model.headers {
            for (name, value) in model_headers.as_map() {
                set_header(&mut headers, name, value)?;
            }
        }
        // Registry/auth resolution may already include classified model headers.
        // Direct resolved calls still receive the model defaults above; final
        // request values, including explicit null, always win below.
        for (name, value) in request.auth.headers.iter_for_dispatch() {
            match value {
                Some(value) => set_header(&mut headers, name, value)?,
                None => {
                    // Validate deleted names too, as Fetch Headers.delete does.
                    let mut probe = HttpHeaders::new();
                    probe.set(name, "")?;
                    headers.remove(name);
                }
            }
        }
        if let Some(session) = payload::cache_session(request)
            && !request
                .resolved
                .model
                .headers
                .as_ref()
                .is_some_and(|headers| {
                    headers
                        .as_map()
                        .keys()
                        .any(|name| name.eq_ignore_ascii_case("x-affinity"))
                })
            && !request
                .auth
                .headers
                .as_map()
                .keys()
                .any(|name| name.eq_ignore_ascii_case("x-affinity"))
        {
            set_header(&mut headers, "x-affinity", session)?;
        }
        let http = HttpRequest {
            endpoint: RequestEndpoint::new_resolved(url.as_str()).map_err(|_| Failure::Protocol)?,
            method: "POST".into(),
            headers,
            body: stringify_wire_json(&wire)
                .map_err(|_| Failure::Protocol)?
                .into_bytes(),
            proxy: None,
        };
        let timeout = timeout_duration(request.options.wire.timeout_ms)?;
        let mut response = self
            .services
            .transport
            .send_fetch(
                &http,
                runtime,
                RetryProfile::None,
                TimeoutProfile::WholeResponse(timeout),
            )
            .await?;
        if let Some(hook) = &self.services.response {
            let head = AdapterResponseHead::from_http(response.status, &response.headers)?;
            cancellable(hook.on_response(request, &head), &runtime.cancellation)
                .await
                .map_err(|_| Failure::Cancelled)?
                .map_err(|_| Failure::Middleware)?;
        }
        if !(200..300).contains(&response.status) {
            let body = read_body_limited(
                response.body.as_mut(),
                runtime,
                self.services.max_error_body_bytes,
            )
            .await?;
            return Err(Failure::Classified(MistralResponseError::from_response(
                response.status,
                &response.status_text,
                &body,
                &reducer.partial,
            )?));
        }
        if matches!(response.status, 204 | 205)
            || response.body.presence() == pi_ai_transport::ResponseBodyPresence::Absent
        {
            response.body.close();
            return Err(Failure::Protocol);
        }
        reducer.emit(
            producer,
            AssistantMessageEvent::Start {
                partial: Box::new(reducer.partial.clone()),
            },
        )?;
        let mut decoder = SseDecoder::new(SseProfile::Mistral, self.services.sse_limits);
        let mut total = 0usize;
        let result = async {
            loop {
                while let Some(frame) = decoder.next_frame().map_err(|_| Failure::Protocol)? {
                    if frame.data == "[DONE]" {
                        return Ok(());
                    }
                    reducer.consume(&request.resolved.model, producer, &frame.data)?;
                }
                match response.body.next(runtime).await? {
                    Some(bytes) => {
                        total = total.checked_add(bytes.len()).ok_or(Failure::Limit)?;
                        if total > self.limits.max_stream_bytes {
                            return Err(Failure::Limit);
                        }
                        decoder.push(&bytes).map_err(|_| Failure::Protocol)?;
                    }
                    None => {
                        decoder.end_input();
                        while let Some(frame) =
                            decoder.next_frame().map_err(|_| Failure::Protocol)?
                        {
                            if frame.data == "[DONE]" {
                                break;
                            }
                            reducer.consume(&request.resolved.model, producer, &frame.data)?;
                        }
                        return Ok(());
                    }
                }
            }
        }
        .await;
        response.body.close();
        result?;
        reducer.finish(producer)?;
        if runtime.cancellation.is_cancelled() {
            return Err(Failure::Cancelled);
        }
        let reason = match reducer.partial.stop_reason {
            StopReason::Stop => DoneReason::Stop,
            StopReason::Length => DoneReason::Length,
            StopReason::ToolUse => DoneReason::ToolUse,
            StopReason::Error => return Err(Failure::AlreadyClassified),
            _ => return Err(Failure::MissingTerminal),
        };
        producer
            .push(AssistantMessageEvent::Done {
                reason,
                message: Box::new(reducer.partial.clone()),
            })
            .map_err(|_| Failure::Cancelled)
    }
}

impl ApiAdapter for MistralApi {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        self.stream_with_options(request, self.defaults.clone())
    }
    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        let mut options = MistralOptions {
            tool_choice: request.options.wire.tool_choice.map(|choice| {
                MistralToolChoice::Mode(match choice {
                    ToolChoice::Auto => MistralToolChoiceMode::Auto,
                    ToolChoice::None => MistralToolChoiceMode::None,
                })
            }),
            ..Default::default()
        };
        let model = &request.resolved.model;
        let mut error = None;
        let mut mapped_effort = None;
        if let Some(level) = request.options.wire.reasoning {
            let level: ModelThinkingLevel =
                serde_json::from_value(serde_json::to_value(level).expect("closed level"))
                    .expect("shared level");
            let level = clamp_thinking_level(model, level);
            if model.reasoning && level != ModelThinkingLevel::Off {
                if matches!(
                    model.id.as_str(),
                    "mistral-small-2603" | "mistral-small-latest" | "mistral-medium-3.5"
                ) {
                    let effort = model
                        .thinking_level_map
                        .as_ref()
                        .and_then(|map| map.as_map().get(&level))
                        .and_then(|value| value.as_deref())
                        .unwrap_or("high");
                    mapped_effort = Some(effort.to_owned());
                } else {
                    options.prompt_mode = Some(MistralPromptMode::Reasoning);
                }
            }
        }
        let wire = build_base_options(model, &request.context, &request.options.wire.stream);
        let wire = match wire {
            Ok(wire) => wire,
            Err(_) => {
                error = Some(Failure::Protocol);
                request.options.wire.stream
            }
        };
        self.start(
            TextStreamRequest {
                resolved: request.resolved,
                context: request.context,
                auth: request.auth,
                options: StreamOptions {
                    wire,
                    runtime: request.options.runtime,
                },
            },
            error.map_or(Ok((options, mapped_effort)), Err),
        )
    }
    fn fetch_deferred(&self, _: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        Err(CapabilityError::Unsupported)
    }
    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async {
            Err(ModelsError::with_source(
                ModelsErrorCode::Stream,
                CapabilityError::Unsupported,
            ))
        })
    }
}

fn set_header(headers: &mut HttpHeaders, name: &str, value: &str) -> Result<(), Failure> {
    if value.chars().any(|c| u32::from(c) > 255) {
        return Err(Failure::Protocol);
    }
    headers.set(name, value.trim_matches([' ', '\t', '\r', '\n']))?;
    Ok(())
}

fn timeout_duration(value: Option<SafeUnsigned>) -> Result<Duration, Failure> {
    let milliseconds = value.map_or(60_000, SafeUnsigned::get);
    // Node AbortSignal.timeout validates unsigned 32-bit integer milliseconds;
    // its underlying timer coerces values above signed 32-bit range to 1 ms.
    if milliseconds > u64::from(u32::MAX) {
        return Err(Failure::Protocol);
    }
    Ok(Duration::from_millis(if milliseconds > i32::MAX as u64 {
        1
    } else {
        milliseconds
    }))
}

enum Failure {
    ApiMismatch,
    Authentication,
    Protocol,
    Limit,
    Middleware,
    MissingTerminal,
    Cancelled,
    AlreadyClassified,
    Transport(TransportErrorKind),
    Classified(MistralResponseError),
}
impl From<TransportError> for Failure {
    fn from(error: TransportError) -> Self {
        Self::Transport(error.kind())
    }
}

fn empty_message(request: &TextStreamRequest) -> AssistantMessage {
    AssistantMessage {
        role: AssistantRole::Assistant,
        content: vec![],
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
        timestamp: SafeUnsigned::new(request.options.runtime.clock.now_unix_ms())
            .unwrap_or(SafeUnsigned::MAX),
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
fn classify(text: &str, template: &AssistantMessage) -> (bool, bool) {
    let mut probe = template.clone();
    probe.stop_reason = StopReason::Error;
    probe.error_message = Some(text.into());
    (
        is_context_overflow(&probe, None),
        is_retryable_assistant_error(&probe),
    )
}

/// Safe HTTP classification; raw reason/body are discarded after source formatting.
#[derive(Clone, Copy, Debug)]
pub struct MistralResponseError {
    pub status: u16,
    pub context_overflow: bool,
    pub retryable: bool,
}
impl MistralResponseError {
    pub fn from_response(
        status: u16,
        status_text: &str,
        body: &[u8],
        template: &AssistantMessage,
    ) -> Result<Self, TransportError> {
        let reason_bytes = status_text
            .chars()
            .map(|c| u8::try_from(u32::from(c)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| TransportError::new(TransportErrorKind::Protocol))?;
        let reason = String::from_utf8_lossy(&reason_bytes);
        let body = String::from_utf8_lossy(body);
        let body = body
            .strip_prefix('\u{feff}')
            .unwrap_or(&body)
            .trim_matches(ecma_whitespace);
        // JS slice/count use UTF-16, including a possible split surrogate at 4000.
        // Replacement of that isolated half cannot create any classifier pattern.
        let units: Vec<u16> = body.encode_utf16().collect();
        let detail = if !body.is_empty() {
            if units.len() > 4000 {
                format!(
                    "{}... [truncated {} chars]",
                    String::from_utf16_lossy(&units[..4000]),
                    units.len() - 4000
                )
            } else {
                body.into()
            }
        } else if reason.is_empty() {
            format!("Request failed with status {status}")
        } else {
            reason.into_owned()
        };
        let (context_overflow, retryable) =
            classify(&format!("Mistral API error ({status}): {detail}"), template);
        Ok(Self {
            status,
            context_overflow,
            retryable,
        })
    }
}
impl fmt::Display for MistralResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Numeric status can re-enable retry after billing/quota text was
        // removed. Keep classifier-facing text entirely semantic.
        f.write_str(classified_message(self.context_overflow, self.retryable))
    }
}
impl std::error::Error for MistralResponseError {}

fn publish_failure(
    request: &TextStreamRequest,
    producer: &AssistantStreamProducer,
    mut partial: AssistantMessage,
    failure: Failure,
) {
    let aborted = request.options.runtime.cancellation.is_cancelled();
    if !aborted && let Failure::Classified(error) = &failure {
        partial.diagnostics.get_or_insert_with(Vec::new).push(AssistantMessageDiagnostic {
            diagnostic_type: "mistral_http_failure".parse().expect("constant diagnostic type"),
            timestamp: partial.timestamp,
            error: None,
            details: Some(serde_json::from_value(serde_json::json!({"status":error.status,"contextOverflow":error.context_overflow,"retryable":error.retryable})).expect("safe HTTP diagnostic")),
        });
    }
    let http_text = match &failure {
        Failure::Classified(error) => Some(error.to_string()),
        _ => None,
    };
    let text = match failure {
        Failure::Classified(_) => http_text.as_deref().expect("classified HTTP text"),
        Failure::AlreadyClassified => partial
            .error_message
            .as_deref()
            .unwrap_or("provider request failed"),
        Failure::Transport(TransportErrorKind::Timeout) => "request timeout",
        Failure::Transport(TransportErrorKind::Connection) => "network error",
        Failure::Authentication => "Mistral API key is missing",
        Failure::ApiMismatch => "Mistral API mismatch",
        Failure::MissingTerminal => "Mistral stream ended without a finish reason",
        Failure::Middleware => "Mistral middleware failed",
        Failure::Limit => "Mistral resource limit exceeded",
        _ => "invalid Mistral protocol",
    };
    partial.error_message = Some(
        if aborted {
            REQUEST_CANCELLED_MESSAGE
        } else {
            text
        }
        .into(),
    );
    partial.stop_reason = if aborted {
        StopReason::Aborted
    } else {
        StopReason::Error
    };
    let _ = producer.push(AssistantMessageEvent::Error {
        reason: if aborted {
            ErrorReason::Aborted
        } else {
            ErrorReason::Error
        },
        error: Box::new(partial),
    });
}

fn ecma_whitespace(c: char) -> bool {
    matches!(c, '\u{9}'..='\u{d}' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' |
        '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}
