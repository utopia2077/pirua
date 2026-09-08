//! Pinned Anthropic Messages request and custom raw-response stream behavior.
//!
//! SDK-equivalent unsuccessful response processing occurs inside the sole retry
//! owner. Successful streaming bodies are never retried. All default errors are
//! classified before redaction; cancellation retains the latest owned snapshot.

use crate::api::runtime::{AdapterResponseHead, AdapterServices, cancellable};
use crate::api::simple_options::{
    adjust_max_tokens_for_thinking, build_base_options, clamp_max_tokens_to_context,
};
use pi_ai_core::*;
use pi_ai_transport::{
    HttpResponse, ProviderAttemptFailure, ProviderRetryError, RetryProfile, SseDecoder, SseProfile,
    TimeoutProfile, TransportErrorKind, read_body_limited,
};
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

mod payload;
mod reducer;
mod request;

fn optional_non_null<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(deserializer)?
        .map(Some)
        .ok_or_else(|| serde::de::Error::custom("expected a non-null optional value"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AnthropicEffort {
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AnthropicThinkingDisplay {
    Summarized,
    Omitted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AnthropicToolChoiceMode {
    Auto,
    Any,
    None,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AnthropicToolChoiceType {
    Tool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum AnthropicToolChoice {
    Mode(AnthropicToolChoiceMode),
    Named {
        #[serde(rename = "type")]
        choice_type: AnthropicToolChoiceType,
        name: String,
    },
}

/// Family-owned raw fields. Neutral fields remain on the resolved request;
/// an alternative client is a trusted construction-time capability, not data.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnthropicOptions {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub thinking_enabled: Option<bool>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub thinking_budget_tokens: Option<SafeUnsigned>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub effort: Option<AnthropicEffort>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub thinking_display: Option<AnthropicThinkingDisplay>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub interleaved_thinking: Option<bool>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub tool_choice: Option<AnthropicToolChoice>,
}

#[derive(Clone, Copy, Debug)]
pub struct AnthropicLimits {
    pub max_content_blocks: usize,
    pub max_stream_bytes: usize,
    pub max_tool_input_bytes: usize,
}
impl Default for AnthropicLimits {
    fn default() -> Self {
        Self {
            max_content_blocks: 4096,
            max_stream_bytes: 16 * 1024 * 1024,
            max_tool_input_bytes: 262_144,
        }
    }
}

enum Failure {
    Authentication,
    Protocol,
    Limit,
    Middleware,
    MissingTerminal,
    Cancelled,
    Classified(AnthropicResponseError),
    RetryCap(AnthropicResponseError),
}

/// Safe semantic projection of an SDK or stream error. Raw text is discarded.
#[derive(Clone, Copy, Debug)]
pub struct AnthropicResponseError {
    pub status: Option<u16>,
    pub context_overflow: bool,
    pub retryable: bool,
    retry_delay_retryable: bool,
    retry_delay_context_overflow: bool,
}
impl AnthropicResponseError {
    /// Trusted custom clients classify their complete SDK error before returning.
    pub fn from_message(status: Option<u16>, message: &str, template: &AssistantMessage) -> Self {
        let mut probe = template.clone();
        probe.stop_reason = StopReason::Error;
        probe.error_message = Some(message.into());
        let context_overflow = is_context_overflow(&probe, None);
        let retryable = is_retryable_assistant_error(&probe);
        probe.error_message = Some(format!("retry delay. {message}"));
        Self {
            status,
            context_overflow,
            retryable,
            retry_delay_retryable: is_retryable_assistant_error(&probe),
            retry_delay_context_overflow: is_context_overflow(&probe, None),
        }
    }
    pub fn from_response(
        status: u16,
        body: &[u8],
        template: &AssistantMessage,
    ) -> Result<Self, pi_ai_transport::TransportError> {
        let text = String::from_utf8_lossy(body);
        let text = text.strip_prefix('\u{feff}').unwrap_or(&text);
        let parsed = serde_json::from_str::<serde_json::Value>(text).ok();
        let message = if let Some(value) = parsed.as_ref().filter(|value| truthy(value)) {
            let value = value
                .get("message")
                .filter(|value| truthy(value))
                .unwrap_or(value);
            if let Some(text) = value.as_str().filter(|_| {
                parsed
                    .as_ref()
                    .and_then(|v| v.get("message"))
                    .is_some_and(truthy)
            }) {
                text.into()
            } else {
                crate::api::wire_json::stringify_wire_json(value).map_err(|_| {
                    pi_ai_transport::TransportError::new(
                        pi_ai_transport::TransportErrorKind::Protocol,
                    )
                })?
            }
        } else {
            text.into()
        };
        let message = if message.is_empty() {
            format!("{status} status code (no body)")
        } else {
            format!("{status} {message}")
        };
        Ok(Self::from_message(Some(status), &message, template))
    }
    fn safe_message(&self, delay: bool) -> String {
        let retryable = if delay {
            self.retry_delay_retryable
        } else {
            self.retryable
        };
        let overflow = if delay {
            self.retry_delay_context_overflow
        } else {
            self.context_overflow
        };
        let detail = match (overflow, retryable) {
            (true, true) => "prompt is too long; server error",
            (true, false) => "prompt is too long",
            (false, true) => "server error",
            (false, false) => "provider request failed",
        };
        // Status belongs in structured diagnostics. Putting 429/503 into this
        // string would undo a source billing/quota negative retry veto.
        detail.into()
    }
}
impl std::fmt::Display for AnthropicResponseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.safe_message(false))
    }
}
impl std::error::Error for AnthropicResponseError {}
fn truthy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(v) => *v,
        serde_json::Value::Number(n) => n.as_f64().is_some_and(|v| v != 0.0),
        serde_json::Value::String(s) => !s.is_empty(),
        _ => true,
    }
}

/// Explicit options supplied to one trusted client attempt. Lower retries cannot
/// inherit the neutral request's retry budget; only the outer owner retries.
pub struct AnthropicClientOptions<'a> {
    pub timeout_ms: Option<SafeUnsigned>,
    pub cancellation: &'a CancellationToken,
}
impl AnthropicClientOptions<'_> {
    pub fn max_retries(&self) -> u64 {
        0
    }
}
impl fmt::Debug for AnthropicClientOptions<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AnthropicClientOptions([REDACTED])")
    }
}

/// Source `options.client` capability captured at construction, never serialized.
/// Each call is a complete single SDK-equivalent attempt: unsuccessful response
/// bodies must be consumed/classified before returning Provider failure. Disable
/// inner retries, honor original cancellation, and release I/O on future drop.
pub trait AnthropicClient: Send + Sync {
    fn create<'a>(
        &'a self,
        request: &'a TextStreamRequest,
        payload: &'a super::wire_json::WirePayload,
        options: AnthropicClientOptions<'a>,
    ) -> BoxFuture<'a, Result<HttpResponse, ProviderAttemptFailure<AnthropicResponseError>>>;
}

/// Passive, default-off Messages adapter. A supplied client bypasses internal
/// SDK-equivalent auth/header/URL setup and disables OAuth payload shaping.
#[derive(Clone)]
pub struct AnthropicMessagesApi {
    api: ApiId,
    pub services: AdapterServices,
    pub defaults: AnthropicOptions,
    pub limits: AnthropicLimits,
    pub client: Option<Arc<dyn AnthropicClient>>,
}
impl fmt::Debug for AnthropicMessagesApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AnthropicMessagesApi([REDACTED])")
    }
}
impl AnthropicMessagesApi {
    pub fn new(services: AdapterServices) -> Self {
        Self {
            api: "anthropic-messages".parse().expect("constant API"),
            services,
            defaults: AnthropicOptions::default(),
            limits: AnthropicLimits::default(),
            client: None,
        }
    }
    pub fn stream_with_options(
        &self,
        request: TextStreamRequest,
        options: AnthropicOptions,
    ) -> AssistantStream {
        self.start(request, Ok((options, None)))
    }
    fn start(
        &self,
        request: TextStreamRequest,
        options: Result<(AnthropicOptions, Option<String>), Failure>,
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
            publish_failure(&request, &producer, partial, Failure::Protocol);
            return stream;
        }
        let adapter = self.clone();
        let spawner = request.options.runtime.spawner.clone();
        let worker = producer.clone();
        if spawner
            .spawn(Box::pin(async move {
                let mut reducer =
                    reducer::Reducer::new(partial, request.resolved.model.clone(), adapter.limits);
                let result = match options {
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
                if let Err(error) = result {
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
        options: &AnthropicOptions,
        mapped_effort: Option<&str>,
        producer: &AssistantStreamProducer,
        reducer: &mut reducer::Reducer,
    ) -> Result<(), Failure> {
        let runtime = &request.options.runtime;
        if self.client.is_none() {
            request::assert_auth(request)?;
        }
        let oauth = self.client.is_none() && request::is_oauth(request);
        let mut params = super::wire_json::WirePayload::new(payload::build(
            request,
            options,
            mapped_effort,
            &self.services,
            oauth,
        )?);
        if let Some(hook) = &self.services.payload
            && let Some(replacement) =
                cancellable(hook.on_payload(request, &mut params), &runtime.cancellation)
                    .await
                    .map_err(|_| Failure::Cancelled)?
                    .map_err(|_| Failure::Middleware)?
        {
            params = replacement;
        }
        let wire = request::force_stream(params.into_value())?;
        let params = super::wire_json::WirePayload::new(wire.clone());
        let http = if self.client.is_none() {
            Some(request::build(request, options, &self.services, &wire)?)
        } else {
            None
        };
        let template = &reducer.partial;
        let mut response=self.services.transport.retry_provider_request(runtime,RetryProfile::SharedProvider{
            max_retries:request.options.wire.max_retries.map_or(0,SafeUnsigned::get),max_retry_delay_ms:request.options.wire.max_retry_delay_ms.map(SafeUnsigned::get)
        },||->BoxFuture<'_,Result<HttpResponse,ProviderAttemptFailure<AnthropicResponseError>>>{
            Box::pin(async{
                if let Some(client)=&self.client{return client.create(request,&params,AnthropicClientOptions{timeout_ms:request.options.wire.timeout_ms,cancellation:&runtime.cancellation}).await;}
                let http=http.as_ref().expect("default client request");
                let mut response=self.services.transport.send_fetch(http,runtime,RetryProfile::None,TimeoutProfile::Headers(request::timeout(request))).await
                    .map_err(|error|{
                        // SDK fetch rejection is wrapped as APIConnectionError,
                        // including redirect/protocol failures from Fetch itself.
                        let detail=if error.kind()==TransportErrorKind::Timeout{"Request timed out."}
                            else if runtime.cancellation.is_cancelled(){"Request was aborted."}else{"Connection error."};
                        let error=AnthropicResponseError::from_message(None,detail,template);
                        ProviderAttemptFailure::Provider{status:None,headers:None,error}
                    })?;
                if !(200..300).contains(&response.status){
                    let body=read_body_limited(response.body.as_mut(),runtime,self.services.max_error_body_bytes).await;
                    let error=match body {
                        Ok(body)=>AnthropicResponseError::from_response(response.status,&body,template).map_err(|_|ProviderAttemptFailure::Other(AnthropicResponseError::from_message(Some(response.status),"invalid Anthropic error body",template)))?,
                        Err(error) if error.kind()==TransportErrorKind::LimitExceeded=>return Err(ProviderAttemptFailure::Other(AnthropicResponseError::from_message(Some(response.status),"Anthropic error body resource limit exceeded",template))),
                        // SDK response.text() catches read errors and still constructs
                        // a status/headers-bearing APIError before the outer retry.
                        Err(error)=>AnthropicResponseError::from_response(response.status,transport_message(error.kind()).as_bytes(),template).expect("fixed SDK read error"),
                    };
                    return Err(ProviderAttemptFailure::Provider{status:Some(response.status),headers:Some(response.headers.clone()),error});
                }
                Ok(response)
            })
        }).await.map_err(|error|match error{
            ProviderRetryError::Attempt(error)=>Failure::Classified(attempt_error(error)),
            ProviderRetryError::Cancelled{..}=>Failure::Cancelled,
            ProviderRetryError::Policy{error,previous}=>match previous{
                Some(previous) if error.kind()==TransportErrorKind::RetryDelayExceeded=>Failure::RetryCap(attempt_error(previous)),
                Some(previous)=>Failure::Classified(attempt_error(previous)),None=>Failure::Protocol,
            }
        })?;
        if let Some(hook) = &self.services.response {
            let head = AdapterResponseHead::from_http(response.status, &response.headers)?;
            cancellable(hook.on_response(request, &head), &runtime.cancellation)
                .await
                .map_err(|_| Failure::Cancelled)?
                .map_err(|_| Failure::Middleware)?;
        }
        reducer.emit(
            producer,
            AssistantMessageEvent::Start {
                partial: Box::new(reducer.partial.clone()),
            },
        )?;
        if matches!(response.status, 204 | 205) {
            return Err(Failure::Protocol);
        }
        // This is the pinned adapter's private parser profile, NOT the SDK's
        // iterator. Transport owns its push-boundary-sensitive CR/EOF behavior.
        let mut decoder = SseDecoder::new(SseProfile::Anthropic, self.services.sse_limits);
        let mut bytes_read = 0usize;
        let result = async {
            loop {
                while let Some(frame) = decoder.next_frame().map_err(|_| Failure::Protocol)? {
                    reducer.consume(request, producer, oauth, &frame)?;
                }
                match cancellable(response.body.next(runtime), &runtime.cancellation)
                    .await
                    .map_err(|_| Failure::Cancelled)?
                {
                    Ok(Some(bytes)) => {
                        bytes_read = bytes_read.checked_add(bytes.len()).ok_or(Failure::Limit)?;
                        if bytes_read > self.limits.max_stream_bytes {
                            return Err(Failure::Limit);
                        }
                        decoder.push(&bytes).map_err(|_| Failure::Protocol)?;
                    }
                    Ok(None) => {
                        decoder.end_input();
                        while let Some(frame) =
                            decoder.next_frame().map_err(|_| Failure::Protocol)?
                        {
                            reducer.consume(request, producer, oauth, &frame)?;
                        }
                        return Ok(());
                    }
                    Err(error) => {
                        return Err(Failure::Classified(AnthropicResponseError::from_message(
                            None,
                            transport_message(error.kind()),
                            &reducer.partial,
                        )));
                    }
                }
            }
        }
        .await;
        response.body.close();
        result?;
        if runtime.cancellation.is_cancelled() {
            return Err(Failure::Cancelled);
        }
        let reason = reducer.finish()?;
        producer
            .push(AssistantMessageEvent::Done {
                reason,
                message: Box::new(reducer.partial.clone()),
            })
            .map_err(|_| Failure::Cancelled)
    }
}

impl ApiAdapter for AnthropicMessagesApi {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        self.stream_with_options(request, self.defaults.clone())
    }
    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        let mut options = AnthropicOptions {
            thinking_enabled: Some(request.options.wire.reasoning.is_some()),
            tool_choice: request.options.wire.tool_choice.map(|choice| {
                AnthropicToolChoice::Mode(match choice {
                    ToolChoice::Auto => AnthropicToolChoiceMode::Auto,
                    ToolChoice::None => AnthropicToolChoiceMode::None,
                })
            }),
            ..Default::default()
        };
        let model = &request.resolved.model;
        let mut failure = None;
        let mut mapped_effort = None;
        let mut wire = build_base_options(model, &request.context, &request.options.wire.stream)
            .unwrap_or_else(|_| {
                failure = Some(Failure::Protocol);
                request.options.wire.stream.clone()
            });
        if let Some(level) = request.options.wire.reasoning {
            if compat(model)
                .map(|compat| compat.force_adaptive_thinking == Some(true))
                .unwrap_or(false)
            {
                let mapped_level: ModelThinkingLevel =
                    serde_json::from_value(serde_json::to_value(level).expect("closed level"))
                        .expect("shared level");
                let mapped = model
                    .thinking_level_map
                    .as_ref()
                    .and_then(|map| map.as_map().get(&mapped_level))
                    .and_then(|v| v.clone());
                mapped_effort = Some(mapped.unwrap_or_else(|| {
                    match level {
                        ThinkingLevel::Minimal | ThinkingLevel::Low => "low",
                        ThinkingLevel::Medium => "medium",
                        _ => "high",
                    }
                    .into()
                }));
            } else {
                let adjusted = adjust_max_tokens_for_thinking(
                    wire.max_tokens,
                    model.max_tokens,
                    level,
                    request.options.wire.thinking_budgets.as_ref(),
                );
                match clamp_max_tokens_to_context(model, &request.context, adjusted.max_tokens) {
                    Ok(max) => {
                        wire.max_tokens = Some(max);
                        options.thinking_budget_tokens = Some(
                            SafeUnsigned::new(
                                adjusted
                                    .thinking_budget
                                    .get()
                                    .min(max.get().saturating_sub(1024)),
                            )
                            .expect("safe thinking budget"),
                        );
                    }
                    Err(_) => failure = Some(Failure::Protocol),
                }
            }
        }
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
            failure.map_or(Ok((options, mapped_effort)), Err),
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

fn attempt_error(error: ProviderAttemptFailure<AnthropicResponseError>) -> AnthropicResponseError {
    match error {
        ProviderAttemptFailure::Provider { error, .. } | ProviderAttemptFailure::Other(error) => {
            error
        }
    }
}
fn transport_message(kind: TransportErrorKind) -> &'static str {
    match kind {
        TransportErrorKind::Timeout => "Request timed out.",
        TransportErrorKind::Cancelled => "Request was aborted.",
        TransportErrorKind::Connection => "Connection error.",
        TransportErrorKind::LimitExceeded => "Anthropic resource limit exceeded",
        _ => "Anthropic transport failure",
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
fn publish_failure(
    request: &TextStreamRequest,
    producer: &AssistantStreamProducer,
    mut partial: AssistantMessage,
    failure: Failure,
) {
    let aborted = request.options.runtime.cancellation.is_cancelled();
    let classified = match &failure {
        Failure::Classified(error) => Some((*error, false)),
        Failure::RetryCap(error) => Some((*error, true)),
        _ => None,
    };
    let text = match &failure {
        Failure::Authentication => "Anthropic authentication is missing",
        Failure::Limit => "Anthropic resource limit exceeded",
        Failure::Middleware => "Anthropic middleware failed",
        Failure::MissingTerminal => "Anthropic stream ended without a stop reason",
        Failure::Cancelled => REQUEST_CANCELLED_MESSAGE,
        _ => "invalid Anthropic protocol",
    };
    partial.error_message = Some(if aborted {
        REQUEST_CANCELLED_MESSAGE.into()
    } else {
        classified.map_or_else(|| text.into(), |(error, delay)| error.safe_message(delay))
    });
    if !aborted && let Some((error, delay)) = classified {
        partial.diagnostics=Some(vec![AssistantMessageDiagnostic{diagnostic_type:"anthropic_response_failure".parse().expect("constant diagnostic"),timestamp:partial.timestamp,error:None,
            details:Some(serde_json::from_value(serde_json::json!({"version":1,"status":error.status,"contextOverflow":if delay{error.retry_delay_context_overflow}else{error.context_overflow},
                "retryable":if delay{error.retry_delay_retryable}else{error.retryable},"retryDelayExceeded":delay})).expect("safe diagnostic"))}]);
    }
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

fn compat(model: &Model) -> Result<AnthropicMessagesCompat, Failure> {
    match model
        .compat
        .as_ref()
        .map(|compat| compat.decode_for_api(&model.api))
        .transpose()
        .map_err(|_| Failure::Protocol)?
    {
        None => Ok(AnthropicMessagesCompat::default()),
        Some(TypedModelCompat::AnthropicMessages(compat)) => Ok(*compat),
        _ => Err(Failure::Protocol),
    }
}

fn ecma_whitespace(c: char) -> bool {
    matches!(c, '\u{9}'..='\u{d}' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' |
        '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}
