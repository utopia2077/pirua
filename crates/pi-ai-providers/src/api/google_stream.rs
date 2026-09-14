//! Canonical Google Generative AI adapter, including SDK API-key Express routing.
use super::{
    google_sdk as sdk, google_shared as shared,
    runtime::{AdapterServices, cancellable},
    simple_options::build_base_options,
    wire_json::{project_wire_safe_unsigned, stringify_wire_json},
};
use pi_ai_core::*;
use pi_ai_transport::{
    ProviderAttemptFailure, ProviderRetryError, RetryProfile, TimeoutProfile, TransportErrorKind,
    read_body_limited,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

fn nonnull<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(d)?
        .map(Some)
        .ok_or_else(|| serde::de::Error::custom("expected a non-null optional field"))
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GoogleToolChoice {
    Auto,
    None,
    Any,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoogleThinking {
    pub enabled: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub budget_tokens: Option<FiniteF64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub level: Option<shared::GoogleApiThinkingLevel>,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoogleOptions {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub tool_choice: Option<GoogleToolChoice>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub thinking: Option<GoogleThinking>,
}
static TOOL_CALL_COUNTER: AtomicU64 = AtomicU64::new(0);
static VERTEX_TOOL_CALL_COUNTER: AtomicU64 = AtomicU64::new(0);
/// Explicit trusted counter injection. Default adapters share the module counter.
pub struct GoogleToolCallCounter(AtomicU64);
impl GoogleToolCallCounter {
    pub fn new(initial: SafeUnsigned) -> Self {
        Self(AtomicU64::new(initial.get()))
    }
}
impl fmt::Debug for GoogleToolCallCounter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GoogleToolCallCounter([REDACTED])")
    }
}
#[derive(Clone, Copy, Debug)]
pub struct GoogleGenAiLimits {
    pub max_content_blocks: usize,
    pub max_stream_bytes: usize,
    pub max_tool_argument_bytes: usize,
}
impl Default for GoogleGenAiLimits {
    fn default() -> Self {
        Self {
            max_content_blocks: 4096,
            max_stream_bytes: 16 * 1024 * 1024,
            max_tool_argument_bytes: 262144,
        }
    }
}
#[derive(Clone)]
pub struct GoogleGenAiApi {
    api: ApiId,
    pub services: AdapterServices,
    pub defaults: GoogleOptions,
    pub limits: GoogleGenAiLimits,
    counter: Option<Arc<GoogleToolCallCounter>>,
    #[cfg(feature = "wire-google-vertex")]
    vertex: Option<super::google_vertex::Invocation>,
}
impl fmt::Debug for GoogleGenAiApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GoogleGenAiApi([REDACTED])")
    }
}
#[derive(Clone, Copy)]
struct Failure {
    status: Option<u16>,
    retry: bool,
    overflow: bool,
    cap_retry: bool,
    cap_overflow: bool,
}
impl Failure {
    fn local() -> Self {
        Self {
            status: None,
            retry: false,
            overflow: false,
            cap_retry: false,
            cap_overflow: false,
        }
    }
    fn message(status: Option<u16>, message: &str, template: &AssistantMessage) -> Self {
        let mut probe = template.clone();
        probe.stop_reason = StopReason::Error;
        probe.error_message = Some(message.into());
        let retry = is_retryable_assistant_error(&probe);
        let overflow = is_context_overflow(&probe, None);
        probe.error_message = Some(format!("Server requested retry delay. {message}"));
        Self {
            status,
            retry,
            overflow,
            cap_retry: is_retryable_assistant_error(&probe),
            cap_overflow: is_context_overflow(&probe, None),
        }
    }
    fn sdk(error: sdk::Error, template: &AssistantMessage) -> Self {
        match error {
            sdk::Error::Message {
                status, message, ..
            } => Self::message(status, &message, template),
            _ => Self::local(),
        }
    }
    fn safe(self) -> &'static str {
        match (self.overflow, self.retry) {
            (true, true) => "prompt is too long; server error",
            (true, false) => "prompt is too long",
            (false, true) => "server error",
            _ => "Google provider request failed",
        }
    }
    fn cap(mut self) -> Self {
        self.retry = self.cap_retry;
        self.overflow = self.cap_overflow;
        self
    }
}
impl GoogleGenAiApi {
    pub fn new(services: AdapterServices) -> Self {
        Self {
            api: "google-generative-ai".parse().expect("constant API"),
            services,
            defaults: GoogleOptions::default(),
            limits: GoogleGenAiLimits::default(),
            counter: None,
            #[cfg(feature = "wire-google-vertex")]
            vertex: None,
        }
    }
    pub fn with_tool_call_counter(mut self, counter: Arc<GoogleToolCallCounter>) -> Self {
        self.counter = Some(counter);
        self
    }
    #[cfg(feature = "wire-google-vertex")]
    pub(crate) fn vertex(
        services: AdapterServices,
        invocation: super::google_vertex::Invocation,
    ) -> Self {
        let mut engine = Self::new(services);
        engine.api = "google-vertex".parse().expect("constant API");
        engine.vertex = Some(invocation);
        engine
    }
    fn is_vertex(&self) -> bool {
        self.api.as_str() == "google-vertex"
    }
    pub fn stream_with_options(
        &self,
        request: TextStreamRequest,
        options: GoogleOptions,
    ) -> AssistantStream {
        self.start(request, Ok(options))
    }
    fn next_tool_id(&self, name: &str, request: &TextStreamRequest) -> Result<ToolCallId, Failure> {
        let counter = self
            .counter
            .as_ref()
            .map(|c| &c.0)
            .unwrap_or(if self.is_vertex() {
                &VERTEX_TOOL_CALL_COUNTER
            } else {
                &TOOL_CALL_COUNTER
            });
        let previous = counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                v.checked_add(1).filter(|v| *v <= SafeUnsigned::MAX.get())
            })
            .map_err(|_| Failure::local())?;
        format!(
            "{name}_{}_{}",
            request.options.runtime.clock.now_unix_ms(),
            previous + 1
        )
        .parse()
        .map_err(|_| Failure::local())
    }
    fn start(
        &self,
        request: TextStreamRequest,
        options: Result<GoogleOptions, Failure>,
    ) -> AssistantStream {
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: request.resolved.model.api.clone(),
            provider: request.resolved.model.provider.clone(),
            model: request.resolved.model.id.clone(),
            clock: request.options.runtime.clock.clone(),
            cancellation: request.options.runtime.cancellation.clone(),
        });
        let partial = empty(&request);
        if request.resolved.model.api != self.api {
            publish_error(&request, &producer, partial, Failure::local());
            return stream;
        }
        let worker = producer.clone();
        let adapter = self.clone();
        let spawner = request.options.runtime.spawner.clone();
        if spawner
            .spawn(Box::pin(async move {
                let mut reducer = Reducer {
                    partial,
                    current: None,
                };
                let result = match options {
                    Ok(options) => adapter.run(&request, &options, &worker, &mut reducer).await,
                    Err(e) => Err(e),
                };
                if let Err(error) = result {
                    publish_error(&request, &worker, reducer.partial, error);
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
        options: &GoogleOptions,
        producer: &AssistantStreamProducer,
        reducer: &mut Reducer,
    ) -> Result<(), Failure> {
        if !self.is_vertex()
            && request
                .auth
                .api_key
                .as_ref()
                .is_none_or(|k| k.expose_secret().is_empty())
        {
            return Err(Failure::local());
        }
        #[cfg(feature = "wire-google-vertex")]
        let vertex_client = self
            .vertex
            .as_ref()
            .map(|v| v.client(request, &self.services.env))
            .transpose()
            .map_err(|_| Failure::local())?;
        let mut params = super::wire_json::WirePayload::new(build_params(request, options)?);
        if let Some(hook) = &self.services.payload
            && let Some(replacement) = cancellable(
                hook.on_payload(request, &mut params),
                &request.options.runtime.cancellation,
            )
            .await
            .map_err(|_| Failure::local())?
            .map_err(|_| Failure::local())?
        {
            params = replacement;
        }
        if let Some(handler) = &self.services.request_payload
            && let Some(replacement) = cancellable(
                super::request_lifecycle::transform(handler.as_ref(), request, &mut params),
                &request.options.runtime.cancellation,
            )
            .await
            .map_err(|_| Failure::local())?
            .map_err(|_| Failure::local())?
        {
            params = replacement;
        }
        let params = params.into_value();
        let template = &reducer.partial;
        let runtime = &request.options.runtime;
        let (mut response, cloud) = shared::retry_google_request(
            &self.services.transport,
            runtime,
            shared::GoogleRetryOptions {
                max_retries: request.options.wire.max_retries,
                max_retry_delay_ms: request.options.wire.max_retry_delay_ms,
            },
            || -> BoxFuture<'_, _> {
                Box::pin(async {
                    let prepared = sdk::prepare(
                        request,
                        &params,
                        &self.services.env,
                        #[cfg(feature = "wire-google-vertex")]
                        vertex_client.as_ref().map(|c| &c.routing),
                    )
                    .map_err(|e| shared::GoogleRequestFailure::Other(Failure::sdk(e, template)))?;
                    #[cfg(feature = "wire-google-vertex")]
                    let prepared = if let Some(client) = &vertex_client {
                        use super::google_vertex::GoogleVertexAuthError;
                        let mut prepared = prepared;
                        if let Some(session) = &client.session {
                            if prepared
                                .request
                                .headers
                                .iter_for_wire()
                                .any(|(_, v)| v.chars().any(|c| c as u32 > 255))
                            {
                                return Err(shared::GoogleRequestFailure::Other(Failure::local()));
                            }
                            let began = runtime.clock.now_unix_ms();
                            let headers = cancellable(
                                session.get_request_headers(&prepared.request.endpoint, runtime),
                                &runtime.cancellation,
                            )
                            .await
                            .map_err(|_| shared::GoogleRequestFailure::Other(Failure::local()))?
                            .map_err(|e| match e {
                                GoogleVertexAuthError::Other(_) => {
                                    shared::GoogleRequestFailure::Other(Failure::local())
                                }
                                GoogleVertexAuthError::Provider { status, .. } => {
                                    shared::GoogleRequestFailure::ApiError {
                                        status,
                                        error: Failure {
                                            status,
                                            ..Failure::local()
                                        },
                                    }
                                }
                            })?;
                            for (name, entry) in headers.iter_for_fetch() {
                                if prepared.request.headers.get_for_wire(name).is_none() {
                                    // Headers.entries yields Set-Cookie separately. The
                                    // source missing-name check admits only its first entry.
                                    let value = if name == "set-cookie" {
                                        Some(entry.to_owned())
                                    } else {
                                        headers.get_for_fetch(name)
                                    }
                                    .ok_or_else(|| {
                                        shared::GoogleRequestFailure::Other(Failure::local())
                                    })?;
                                    if value.chars().any(|c| c as u32 > 255) {
                                        return Err(shared::GoogleRequestFailure::Other(
                                            Failure::local(),
                                        ));
                                    }
                                    prepared.request.headers.append(name, value).map_err(|_| {
                                        shared::GoogleRequestFailure::Other(Failure::local())
                                    })?;
                                }
                            }
                            if let Some(timeout) = prepared.timeout {
                                let elapsed = std::time::Duration::from_millis(
                                    runtime.clock.now_unix_ms().saturating_sub(began),
                                );
                                prepared.timeout = Some(
                                    timeout
                                        .checked_sub(elapsed)
                                        .filter(|d| !d.is_zero())
                                        .ok_or_else(|| {
                                            shared::GoogleRequestFailure::Other(transport_failure(
                                                TransportErrorKind::Timeout,
                                                template,
                                            ))
                                        })?,
                                );
                            }
                        }
                        prepared
                    } else {
                        prepared
                    };
                    let mut response = self
                        .services
                        .transport
                        .send_fetch(
                            &prepared.request,
                            runtime,
                            RetryProfile::None,
                            prepared
                                .timeout
                                .map_or(TimeoutProfile::None, TimeoutProfile::WholeResponse),
                        )
                        .await
                        .map_err(|e| {
                            shared::GoogleRequestFailure::Other(transport_failure(
                                e.kind(),
                                template,
                            ))
                        })?;
                    if !(200..300).contains(&response.status) {
                        let bytes = read_body_limited(
                            response.body.as_mut(),
                            runtime,
                            self.services.max_error_body_bytes,
                        )
                        .await
                        .map_err(|e| {
                            shared::GoogleRequestFailure::Other(transport_failure(
                                e.kind(),
                                template,
                            ))
                        })?;
                        let error = sdk::http_error(
                            response.status,
                            &response.status_text,
                            &response.headers,
                            &bytes,
                        );
                        let is_api = matches!(error, sdk::Error::Message { api: true, .. });
                        let failure = Failure::sdk(error, template);
                        return Err(if is_api {
                            shared::GoogleRequestFailure::ApiError {
                                status: Some(response.status),
                                error: failure,
                            }
                        } else {
                            shared::GoogleRequestFailure::Other(failure)
                        });
                    }
                    Ok((response, prepared.cloud))
                })
            },
        )
        .await
        .map_err(|e| match e {
            ProviderRetryError::Attempt(e) => attempt_error(e),
            ProviderRetryError::Cancelled { .. } => Failure::local(),
            ProviderRetryError::Policy { error, previous } => {
                let failure = previous.map(attempt_error).unwrap_or_else(Failure::local);
                if error.kind() == TransportErrorKind::RetryDelayExceeded {
                    failure.cap()
                } else {
                    failure
                }
            }
        })?;
        reducer.emit(
            producer,
            AssistantMessageEvent::Start {
                partial: Box::new(reducer.partial.clone()),
            },
        )?;
        let result = async {
            if matches!(response.status, 204 | 205)
                || response.body.presence() == pi_ai_transport::ResponseBodyPresence::Absent
            {
                return Err(Failure::message(
                    None,
                    "Response body is empty",
                    &reducer.partial,
                ));
            }
            let mut decoder = sdk::Decoder::new(self.services.sse_limits);
            let mut bytes_read = 0usize;
            loop {
                while let Some(data) = decoder
                    .next()
                    .map_err(|e| Failure::sdk(e, &reducer.partial))?
                {
                    sdk::frame_metadata(&response.status_text, &data)
                        .map_err(|e| Failure::sdk(e, &reducer.partial))?;
                    let parsed: Value =
                        serde_json::from_str(&data).map_err(|_| Failure::local())?;
                    let chunk = sdk::response(parsed, cloud)
                        .map_err(|e| Failure::sdk(e, &reducer.partial))?;
                    reducer.chunk(self, request, producer, &chunk)?;
                }
                match cancellable(response.body.next(runtime), &runtime.cancellation)
                    .await
                    .map_err(|_| Failure::local())?
                {
                    Ok(Some(bytes)) => {
                        bytes_read = bytes_read
                            .checked_add(bytes.len())
                            .filter(|n| *n <= self.limits.max_stream_bytes)
                            .ok_or_else(Failure::local)?;
                        decoder
                            .push(&bytes)
                            .map_err(|e| Failure::sdk(e, &reducer.partial))?;
                    }
                    Ok(None) => {
                        decoder
                            .finish()
                            .map_err(|e| Failure::sdk(e, &reducer.partial))?;
                        break;
                    }
                    Err(e) => {
                        return Err(
                            if matches!(
                                e.kind(),
                                TransportErrorKind::Connection | TransportErrorKind::Closed
                            ) {
                                Failure::message(None, "terminated", &reducer.partial)
                            } else {
                                transport_failure(e.kind(), &reducer.partial)
                            },
                        );
                    }
                }
            }
            reducer.end_current(producer)?;
            if runtime.cancellation.is_cancelled() {
                return Err(Failure::local());
            }
            let reason = match reducer.partial.stop_reason {
                StopReason::Stop => DoneReason::Stop,
                StopReason::Length => DoneReason::Length,
                StopReason::ToolUse => DoneReason::ToolUse,
                StopReason::Pending => {
                    return Err(Failure::message(
                        None,
                        if self.is_vertex() {
                            "Google Vertex stream ended without a finish reason"
                        } else {
                            "Google stream ended without a finish reason"
                        },
                        &reducer.partial,
                    ));
                }
                _ => {
                    return Err(Failure::message(
                        None,
                        &format!(
                            "Provider stopped with: {}",
                            reducer
                                .partial
                                .raw_stop_reason
                                .as_ref()
                                .map(|s| s.as_str())
                                .unwrap_or("")
                        ),
                        &reducer.partial,
                    ));
                }
            };
            producer
                .push(AssistantMessageEvent::Done {
                    reason,
                    message: Box::new(reducer.partial.clone()),
                })
                .map_err(|_| Failure::local())
        }
        .await;
        response.body.close();
        result
    }
}
impl ApiAdapter for GoogleGenAiApi {
    fn lifecycle_adapter(&self) -> Option<&dyn pi_ai_core::RequestLifecycleAdapter> {
        Some(self)
    }
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        self.stream_with_options(request, self.defaults.clone())
    }
    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        let options = simple_options(&request);
        let wire = build_base_options(
            &request.resolved.model,
            &request.context,
            &request.options.wire.stream,
        );
        let (wire, options) = match wire {
            Ok(wire) => (wire, options),
            Err(_) => (request.options.wire.stream.clone(), Err(Failure::local())),
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
            options,
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
fn empty(request: &TextStreamRequest) -> AssistantMessage {
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
fn publish_error(
    request: &TextStreamRequest,
    producer: &AssistantStreamProducer,
    mut partial: AssistantMessage,
    failure: Failure,
) {
    let aborted = request.options.runtime.cancellation.is_cancelled();
    partial.stop_reason = if aborted {
        StopReason::Aborted
    } else {
        StopReason::Error
    };
    partial.error_message = Some(
        if aborted {
            REQUEST_CANCELLED_MESSAGE
        } else {
            failure.safe()
        }
        .into(),
    );
    if !aborted {
        partial.diagnostics=Some(vec![AssistantMessageDiagnostic {diagnostic_type:"google_response_failure".parse().expect("constant diagnostic"),timestamp:partial.timestamp,error:None,details:Some(serde_json::from_value(json!({"status":failure.status,"contextOverflow":failure.overflow,"retryable":failure.retry})).expect("safe flags"))}]);
    }
    let _ = producer.push(AssistantMessageEvent::Error {
        reason: if aborted {
            ErrorReason::Aborted
        } else {
            ErrorReason::Error
        },
        error: Box::new(partial),
    });
}
fn attempt_error(error: ProviderAttemptFailure<Failure>) -> Failure {
    match error {
        ProviderAttemptFailure::Provider { error, .. } | ProviderAttemptFailure::Other(error) => {
            error
        }
    }
}
fn transport_failure(kind: TransportErrorKind, template: &AssistantMessage) -> Failure {
    match kind {
        TransportErrorKind::Connection | TransportErrorKind::Closed => {
            Failure::message(None, "fetch failed", template)
        }
        TransportErrorKind::Timeout => {
            Failure::message(None, "This operation was aborted", template)
        }
        _ => Failure::local(),
    }
}

fn model_generation(id: &str, kind: &str) -> bool {
    let id = id.to_lowercase();
    id.match_indices("gemini-3").any(|(at, _)| {
        let rest = &id[at + 8..];
        let rest = if let Some(digits) = rest.strip_prefix('.') {
            let n = digits.bytes().take_while(u8::is_ascii_digit).count();
            if n == 0 {
                return false;
            }
            &digits[n..]
        } else {
            rest
        };
        rest.starts_with(kind)
    })
}
fn gemma4(id: &str) -> bool {
    let id = id.to_lowercase();
    id.contains("gemma4") || id.contains("gemma-4")
}
fn flash3(id: &str) -> bool {
    model_generation(id, "-flash")
        || matches!(
            id.to_lowercase().as_str(),
            "gemini-flash-latest" | "gemini-flash-lite-latest"
        )
}
fn build_params(request: &TextStreamRequest, options: &GoogleOptions) -> Result<Value, Failure> {
    let model = &request.resolved.model;
    let mut config = json!({});
    if let Some(v) = request.options.wire.temperature {
        config["temperature"] = json!(v);
    }
    if let Some(v) = request.options.wire.max_tokens {
        config["maxOutputTokens"] = json!(v);
    }
    if let Some(prompt) = request
        .context
        .system_prompt
        .as_ref()
        .filter(|s| !s.is_empty())
    {
        config["systemInstruction"] = json!(prompt);
    }
    let strict = shared::supports_google_strict_tool_sampling(model.id.as_str());
    if let Some(tools) = request.context.tools.as_ref().filter(|t| !t.is_empty()) {
        let choice = options.tool_choice.map(|v| match v {
            GoogleToolChoice::Auto => "auto",
            GoogleToolChoice::None => "none",
            GoogleToolChoice::Any => "any",
        });
        let mode = shared::resolve_google_function_calling_mode(tools, choice, strict)
            .map_err(|_| Failure::local())?;
        config["tools"] = serde_json::to_value(
            shared::convert_tools(tools, false, strict).map_err(|_| Failure::local())?,
        )
        .map_err(|_| Failure::local())?;
        if let Some(mode) = mode {
            config["toolConfig"] = json!({"functionCallingConfig":{"mode":mode}});
        }
    }
    if model.reasoning
        && let Some(thinking) = &options.thinking
    {
        config["thinkingConfig"] = if thinking.enabled {
            let mut c = json!({"includeThoughts":true});
            if let Some(level) = thinking.level {
                c["thinkingLevel"] = json!(level);
            } else if let Some(budget) = thinking.budget_tokens {
                c["thinkingBudget"] = json!(budget);
            }
            c
        } else if model_generation(model.id.as_str(), "-pro") {
            json!({"thinkingLevel":"LOW"})
        } else if flash3(model.id.as_str())
            || (model.api.as_str() != "google-vertex" && gemma4(model.id.as_str()))
        {
            json!({"thinkingLevel":"MINIMAL"})
        } else {
            json!({"thinkingBudget":0})
        };
    }
    let contents = shared::convert_messages(
        model,
        &request.context,
        SafeUnsigned::new(request.options.runtime.clock.now_unix_ms())
            .map_err(|_| Failure::local())?,
    )
    .map_err(|_| Failure::local())?;
    if request.options.runtime.cancellation.is_cancelled() {
        return Err(Failure::local());
    }
    Ok(json!({"model":model.id,"contents":contents,"config":config}))
}
fn simple_options(request: &SimpleStreamRequest) -> Result<GoogleOptions, Failure> {
    let choice = request.options.wire.tool_choice.map(|c| match c {
        ToolChoice::Auto => GoogleToolChoice::Auto,
        ToolChoice::None => GoogleToolChoice::None,
    });
    let mut thinking = GoogleThinking {
        enabled: false,
        budget_tokens: None,
        level: None,
    };
    if let Some(level) = request.options.wire.reasoning {
        let model = &request.resolved.model;
        let level: ModelThinkingLevel =
            serde_json::from_value(json!(level)).map_err(|_| Failure::local())?;
        let resolved =
            shared::resolve_google_thinking_level(model, clamp_thinking_level(model, level))
                .map_err(|_| Failure::local())?;
        thinking.enabled = true;
        use shared::{GoogleApiThinkingLevel as G, ResolvedGoogleThinkingLevel as R};
        if model_generation(model.id.as_str(), "-pro")
            || flash3(model.id.as_str())
            || (model.api.as_str() != "google-vertex" && gemma4(model.id.as_str()))
        {
            thinking.level = Some(if model_generation(model.id.as_str(), "-pro") {
                match resolved {
                    R::Minimal | R::Low => G::Low,
                    _ => G::High,
                }
            } else if gemma4(model.id.as_str()) {
                match resolved {
                    R::Minimal | R::Low => G::Minimal,
                    _ => G::High,
                }
            } else {
                match resolved {
                    R::Minimal => G::Minimal,
                    R::Low => G::Low,
                    R::Medium => G::Medium,
                    R::High => G::High,
                }
            });
        } else {
            let custom =
                request
                    .options
                    .wire
                    .thinking_budgets
                    .as_ref()
                    .and_then(|b| match resolved {
                        R::Minimal => b.minimal,
                        R::Low => b.low,
                        R::Medium => b.medium,
                        R::High => b.high,
                    });
            let budget = custom.map(|v| v.get() as f64).unwrap_or_else(|| {
                let id = model.id.as_str();
                if id.contains("2.5-pro") || id.contains("2.5-flash") {
                    match resolved {
                        R::Minimal => {
                            if model.api.as_str() != "google-vertex"
                                && id.contains("2.5-flash-lite")
                            {
                                512.0
                            } else {
                                128.0
                            }
                        }
                        R::Low => 2048.0,
                        R::Medium => 8192.0,
                        R::High => {
                            if id.contains("2.5-pro") {
                                32768.0
                            } else {
                                24576.0
                            }
                        }
                    }
                } else {
                    -1.0
                }
            });
            thinking.budget_tokens = Some(FiniteF64::new(budget).map_err(|_| Failure::local())?);
        }
    }
    Ok(GoogleOptions {
        tool_choice: choice,
        thinking: Some(thinking),
    })
}
struct Reducer {
    partial: AssistantMessage,
    current: Option<usize>,
}
impl Reducer {
    fn snapshot(&self, producer: &AssistantStreamProducer) -> Result<(), Failure> {
        producer
            .publish_cancellation_snapshot(self.partial.clone())
            .map_err(|_| Failure::local())
    }
    fn emit(
        &self,
        producer: &AssistantStreamProducer,
        event: AssistantMessageEvent,
    ) -> Result<(), Failure> {
        self.snapshot(producer)?;
        producer.push(event).map_err(|_| Failure::local())
    }
    fn end_current(&mut self, producer: &AssistantStreamProducer) -> Result<(), Failure> {
        if let Some(index) = self.current.take() {
            let content_index = SafeUnsigned::new(index as u64).map_err(|_| Failure::local())?;
            let partial = Box::new(self.partial.clone());
            let event = match &self.partial.content[index] {
                AssistantContent::Text(b) => AssistantMessageEvent::TextEnd {
                    content_index,
                    content: b.text.clone(),
                    partial,
                },
                AssistantContent::Thinking(b) => AssistantMessageEvent::ThinkingEnd {
                    content_index,
                    content: b.thinking.clone(),
                    partial,
                },
                _ => return Err(Failure::local()),
            };
            self.emit(producer, event)?;
        }
        Ok(())
    }
    fn chunk(
        &mut self,
        api: &GoogleGenAiApi,
        request: &TextStreamRequest,
        producer: &AssistantStreamProducer,
        chunk: &Value,
    ) -> Result<(), Failure> {
        if self.partial.response_id.is_none()
            && let Some(id) = chunk.get("responseId").filter(|v| sdk::truthy(v))
        {
            self.partial.response_id = Some(
                id.as_str()
                    .ok_or_else(Failure::local)?
                    .parse()
                    .map_err(|_| Failure::local())?,
            );
        }
        let candidate = chunk.get("candidates").and_then(|v| v.get(0));
        if let Some(parts) = candidate
            .and_then(|v| v.pointer("/content/parts"))
            .filter(|v| sdk::truthy(v))
        {
            for part in parts.as_array().ok_or_else(Failure::local)? {
                if let Some(text) = part.get("text") {
                    let text = text.as_str().ok_or_else(Failure::local)?;
                    let thinking = part.get("thought") == Some(&Value::Bool(true));
                    let matching = self.current.is_some_and(|i| {
                        matches!(
                            (&self.partial.content[i], thinking),
                            (AssistantContent::Thinking(_), true)
                                | (AssistantContent::Text(_), false)
                        )
                    });
                    if !matching {
                        self.end_current(producer)?;
                        if self.partial.content.len() >= api.limits.max_content_blocks {
                            return Err(Failure::local());
                        }
                        let index = self.partial.content.len();
                        self.current = Some(index);
                        let content_index =
                            SafeUnsigned::new(index as u64).map_err(|_| Failure::local())?;
                        self.partial.content.push(if thinking {
                            AssistantContent::Thinking(ThinkingContent {
                                content_type: ThinkingContentType::Thinking,
                                thinking: String::new(),
                                thinking_signature: None,
                                redacted: None,
                            })
                        } else {
                            AssistantContent::Text(TextContent {
                                content_type: TextContentType::Text,
                                text: String::new(),
                                text_signature: None,
                            })
                        });
                        self.emit(
                            producer,
                            if thinking {
                                AssistantMessageEvent::ThinkingStart {
                                    content_index,
                                    partial: Box::new(self.partial.clone()),
                                }
                            } else {
                                AssistantMessageEvent::TextStart {
                                    content_index,
                                    partial: Box::new(self.partial.clone()),
                                }
                            },
                        )?;
                    }
                    let index = self.current.ok_or_else(Failure::local)?;
                    let content_index =
                        SafeUnsigned::new(index as u64).map_err(|_| Failure::local())?;
                    let signature = part.get("thoughtSignature").and_then(Value::as_str);
                    match &mut self.partial.content[index] {
                        AssistantContent::Thinking(b) => {
                            b.thinking.push_str(text);
                            b.thinking_signature = shared::retain_thought_signature(
                                b.thinking_signature.as_deref(),
                                signature,
                            )
                            .map(str::to_owned);
                        }
                        AssistantContent::Text(b) => {
                            b.text.push_str(text);
                            b.text_signature = shared::retain_thought_signature(
                                b.text_signature.as_deref(),
                                signature,
                            )
                            .map(str::to_owned);
                        }
                        _ => return Err(Failure::local()),
                    }
                    self.emit(
                        producer,
                        if thinking {
                            AssistantMessageEvent::ThinkingDelta {
                                content_index,
                                delta: text.into(),
                                partial: Box::new(self.partial.clone()),
                            }
                        } else {
                            AssistantMessageEvent::TextDelta {
                                content_index,
                                delta: text.into(),
                                partial: Box::new(self.partial.clone()),
                            }
                        },
                    )?;
                }
                if let Some(call) = part.get("functionCall").filter(|v| sdk::truthy(v)) {
                    self.end_current(producer)?;
                    let provided = call.get("id").filter(|v| sdk::truthy(v));
                    let duplicate = provided.and_then(Value::as_str).is_some_and(|id| {
                        self.partial
                            .content
                            .iter()
                            .any(|b| matches!(b,AssistantContent::ToolCall(c) if c.id.as_str()==id))
                    });
                    let id = if provided.is_none() || duplicate {
                        api.next_tool_id(
                            call.get("name")
                                .and_then(Value::as_str)
                                .unwrap_or("undefined"),
                            request,
                        )?
                    } else {
                        provided
                            .and_then(Value::as_str)
                            .ok_or_else(Failure::local)?
                            .parse()
                            .map_err(|_| Failure::local())?
                    };
                    let name = call
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .parse()
                        .map_err(|_| Failure::local())?;
                    let args = call
                        .get("args")
                        .filter(|v| !v.is_null())
                        .cloned()
                        .unwrap_or(json!({}));
                    let delta = stringify_wire_json(&args).map_err(|_| Failure::local())?;
                    if delta.len() > api.limits.max_tool_argument_bytes
                        || self.partial.content.len() >= api.limits.max_content_blocks
                    {
                        return Err(Failure::local());
                    }
                    let signature = part
                        .get("thoughtSignature")
                        .filter(|v| sdk::truthy(v))
                        .map(|v| v.as_str().map(str::to_owned).ok_or_else(Failure::local))
                        .transpose()?;
                    let tool_call = ToolCall {
                        content_type: ToolCallType::ToolCall,
                        id,
                        name,
                        arguments: serde_json::from_value(args).map_err(|_| Failure::local())?,
                        thought_signature: signature,
                        namespace: None,
                    };
                    let content_index = SafeUnsigned::new(self.partial.content.len() as u64)
                        .map_err(|_| Failure::local())?;
                    self.partial
                        .content
                        .push(AssistantContent::ToolCall(tool_call.clone()));
                    self.emit(
                        producer,
                        AssistantMessageEvent::ToolCallStart {
                            content_index,
                            partial: Box::new(self.partial.clone()),
                        },
                    )?;
                    self.emit(
                        producer,
                        AssistantMessageEvent::ToolCallDelta {
                            content_index,
                            delta,
                            partial: Box::new(self.partial.clone()),
                        },
                    )?;
                    self.emit(
                        producer,
                        AssistantMessageEvent::ToolCallEnd {
                            content_index,
                            tool_call,
                            partial: Box::new(self.partial.clone()),
                        },
                    )?;
                }
            }
        }
        if let Some(reason) = candidate
            .and_then(|v| v.get("finishReason"))
            .filter(|v| sdk::truthy(v))
        {
            let reason = reason.as_str().ok_or_else(Failure::local)?;
            self.partial.raw_stop_reason = Some(reason.parse().map_err(|_| Failure::local())?);
            self.snapshot(producer)?;
            let mapped: shared::GoogleFinishReason = serde_json::from_value(json!(reason))
                .map_err(|_| {
                    Failure::message(
                        None,
                        &format!("Unhandled stop reason: {reason}"),
                        &self.partial,
                    )
                })?;
            self.partial.stop_reason = shared::map_stop_reason(mapped);
            if self.partial.stop_reason == StopReason::Stop
                && self
                    .partial
                    .content
                    .iter()
                    .any(|b| matches!(b, AssistantContent::ToolCall(_)))
            {
                self.partial.stop_reason = StopReason::ToolUse;
            }
        }
        if let Some(usage) = chunk.get("usageMetadata").filter(|v| sdk::truthy(v)) {
            let zero = json!(0);
            let field = |name| usage.get(name).filter(|v| sdk::truthy(v)).unwrap_or(&zero);
            let prompt = field("promptTokenCount");
            let cached = field("cachedContentTokenCount");
            let candidates = field("candidatesTokenCount");
            let thoughts = field("thoughtsTokenCount");
            if matches!(
                candidates,
                Value::String(_) | Value::Array(_) | Value::Object(_)
            ) || matches!(
                thoughts,
                Value::String(_) | Value::Array(_) | Value::Object(_)
            ) {
                return Err(Failure::local());
            }
            let mut next = Usage::zero();
            next.input = count(&json!(number(prompt) - number(cached)))?;
            next.output = count(&json!(number(candidates) + number(thoughts)))?;
            next.cache_read = count(cached)?;
            next.reasoning = Some(count(thoughts)?);
            next.total_tokens = count(field("totalTokenCount"))?;
            calculate_cost(&request.resolved.model, &mut next).map_err(|_| Failure::local())?;
            self.partial.usage = next;
        }
        self.snapshot(producer)
    }
}
fn count(value: &Value) -> Result<SafeUnsigned, Failure> {
    project_wire_safe_unsigned(value).map_err(|_| Failure::local())
}
fn number(value: &Value) -> f64 {
    sdk::number(value)
}

impl pi_ai_core::RequestLifecycleAdapter for GoogleGenAiApi {
    fn stream_with_lifecycle(
        &self,
        request: TextStreamRequest,
        lifecycle: pi_ai_core::RequestLifecycleInvocation,
    ) -> AssistantStream {
        let mut scoped = self.clone();
        super::request_lifecycle::bind(&mut scoped.services, lifecycle);
        scoped.stream(request)
    }
    fn stream_simple_with_lifecycle(
        &self,
        request: SimpleStreamRequest,
        lifecycle: pi_ai_core::RequestLifecycleInvocation,
    ) -> AssistantStream {
        let mut scoped = self.clone();
        super::request_lifecycle::bind(&mut scoped.services, lifecycle);
        scoped.stream_simple(request)
    }
}
