//! Pinned Codex direct-Fetch and persistent WebSocket adapter.
//! Explicit services own credentials, module availability and lifetime. There is
//! no OpenAI SDK request execution, discovery, registration or global cache.
use super::{
    constrained_sampling::create_grammar_tool_input_properties,
    openai_prompt_cache::clamp_openai_prompt_cache_key,
    openai_responses_shared::*,
    runtime::{AdapterResponseHead, AdapterServices, cancellable},
    simple_options::build_base_options,
    wire_json::{WirePayload, stringify_wire_json},
};
use pi_ai_core::*;
use pi_ai_transport::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
mod failure;
mod request;
mod resource;
mod response;
use failure::*;
use resource::*;

fn optional<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(d)?
        .map(Some)
        .ok_or_else(|| serde::de::Error::custom("expected non-null option"))
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CodexReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    #[serde(rename = "xhigh")]
    ExtraHigh,
    Max,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CodexReasoningSummary {
    Auto,
    Concise,
    Detailed,
    Off,
    On,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CodexTextVerbosity {
    Low,
    Medium,
    High,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CodexToolChoice {
    Auto,
    None,
    Required,
}
/// Raw family fields supplement the existing nominal request's neutral options.
#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OpenAICodexResponsesOptions {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional"
    )]
    pub reasoning_effort: Option<CodexReasoningEffort>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub reasoning_summary: Presence<CodexReasoningSummary>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub service_tier: Presence<ResponsesServiceTier>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional"
    )]
    pub text_verbosity: Option<CodexTextVerbosity>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional"
    )]
    pub tool_choice: Option<CodexToolChoice>,
}
impl fmt::Debug for OpenAICodexResponsesOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OpenAICodexResponsesOptions([REDACTED])")
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CodexSourceSignalPresence {
    #[default]
    Present,
    Absent,
}
/// Explicit local storage bounds; no cumulative connection byte limit.
#[derive(Clone, Copy, Debug)]
pub struct CodexResourceLimits {
    pub max_sessions: usize,
    /// Shared retained-operation budget: active leases, idle tasks and retirements.
    pub max_active_leases: usize,
    pub max_stats: usize,
    pub max_key_bytes: usize,
    pub max_continuation_bytes: usize,
    pub max_message_bytes: usize,
    pub close_grace: Duration,
}
impl Default for CodexResourceLimits {
    fn default() -> Self {
        Self {
            max_sessions: 128,
            max_active_leases: 256,
            max_stats: 256,
            max_key_bytes: 8192,
            max_continuation_bytes: 1_048_576,
            max_message_bytes: 1_048_576,
            close_grace: Duration::from_secs(2),
        }
    }
}
/// Supplied runtime bindings, never serialized or discovered by the adapter.
#[derive(Clone)]
pub struct CodexServices {
    pub adapter: AdapterServices,
    pub websocket: Option<Arc<dyn PersistentWebSocketConnector>>,
    pub request_ids: Arc<UuidV7Generator>,
    pub date_parser: Arc<dyn RetryDateParser>,
    pub signal_presence: CodexSourceSignalPresence,
    pub compression: Option<ZstdRequestLimits>,
    pub limits: CodexResourceLimits,
}
impl fmt::Debug for CodexServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CodexServices([REDACTED])")
    }
}
#[derive(Clone)]
pub struct OpenAICodexResponsesApi {
    api: ApiId,
    services: CodexServices,
    resource: Arc<Resource>,
    owner: Option<Arc<ResourceOwner>>,
    pub defaults: OpenAICodexResponsesOptions,
}
impl fmt::Debug for OpenAICodexResponsesApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OpenAICodexResponsesApi([REDACTED])")
    }
}
impl OpenAICodexResponsesApi {
    pub fn new(services: CodexServices) -> Result<Self, ModelsError> {
        let l = services.limits;
        if l.max_active_leases == 0
            || l.max_sessions == 0
            || l.max_stats == 0
            || l.max_key_bytes == 0
            || l.max_continuation_bytes == 0
            || l.max_message_bytes == 0
        {
            return Err(ModelsError::new(ModelsErrorCode::Provider));
        }
        let resource = Arc::new(Resource::new(l));
        Ok(Self {
            api: ApiId::new("openai-codex-responses").expect("constant API"),
            owner: Some(Arc::new(ResourceOwner(Arc::downgrade(&resource)))),
            resource,
            services,
            defaults: Default::default(),
        })
    }
    pub fn stream_with_options(
        &self,
        request: TextStreamRequest,
        options: OpenAICodexResponsesOptions,
    ) -> AssistantStream {
        self.start(request, options, false)
    }
    fn start(
        &self,
        mut request: TextStreamRequest,
        options: OpenAICodexResponsesOptions,
        simple: bool,
    ) -> AssistantStream {
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: request.resolved.model.api.clone(),
            provider: request.resolved.model.provider.clone(),
            model: request.resolved.model.id.clone(),
            clock: request.options.runtime.clock.clone(),
            cancellation: request.options.runtime.cancellation.clone(),
        });
        let mut output = empty_message(&request);
        let mut this = self.clone();
        this.owner = None;
        let worker = producer.clone();
        let spawner = request.options.runtime.spawner.clone();
        let task = Box::pin(async move {
            let result = async {
                if request.resolved.model.api != this.api {
                    return Err(Failure::setup("api_mismatch"));
                }
                if simple {
                    request::key(&request)?;
                    request.options.wire = build_base_options(
                        &request.resolved.model,
                        &request.context,
                        &request.options.wire,
                    )
                    .map_err(|_| Failure::setup("context_estimate"))?;
                }
                this.run(&request, &options, &worker, &mut output).await
            }
            .await;
            if let Err(error) = result {
                publish_failure(&request, &worker, output, error);
            }
        });
        if spawner.spawn(task).is_err() {
            let _ = producer.fail_setup(StreamSetupFailure::Spawn);
        }
        stream
    }
}

impl ApiAdapter for OpenAICodexResponsesApi {
    fn lifecycle_adapter(&self) -> Option<&dyn pi_ai_core::RequestLifecycleAdapter> {
        Some(self)
    }
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, r: TextStreamRequest) -> AssistantStream {
        self.stream_with_options(r, self.defaults.clone())
    }
    fn stream_simple(&self, r: SimpleStreamRequest) -> AssistantStream {
        let effort = r.options.wire.reasoning.and_then(|v| {
            let level = match v {
                ThinkingLevel::Minimal => ModelThinkingLevel::Minimal,
                ThinkingLevel::Low => ModelThinkingLevel::Low,
                ThinkingLevel::Medium => ModelThinkingLevel::Medium,
                ThinkingLevel::High => ModelThinkingLevel::High,
                ThinkingLevel::ExtraHigh => ModelThinkingLevel::ExtraHigh,
                ThinkingLevel::Max => ModelThinkingLevel::Max,
            };
            match clamp_thinking_level(&r.resolved.model, level) {
                ModelThinkingLevel::Off => None,
                ModelThinkingLevel::Minimal => Some(CodexReasoningEffort::Minimal),
                ModelThinkingLevel::Low => Some(CodexReasoningEffort::Low),
                ModelThinkingLevel::Medium => Some(CodexReasoningEffort::Medium),
                ModelThinkingLevel::High => Some(CodexReasoningEffort::High),
                ModelThinkingLevel::ExtraHigh => Some(CodexReasoningEffort::ExtraHigh),
                ModelThinkingLevel::Max => Some(CodexReasoningEffort::Max),
            }
        });
        let options = OpenAICodexResponsesOptions {
            reasoning_effort: effort,
            tool_choice: r.options.wire.tool_choice.map(|v| match v {
                ToolChoice::Auto => CodexToolChoice::Auto,
                ToolChoice::None => CodexToolChoice::None,
            }),
            ..Default::default()
        };
        self.start(
            TextStreamRequest {
                resolved: r.resolved,
                context: r.context,
                auth: r.auth,
                options: StreamOptions {
                    wire: r.options.wire.stream,
                    runtime: r.options.runtime,
                },
            },
            options,
            true,
        )
    }
    fn fetch_deferred(&self, _: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        Err(CapabilityError::Unsupported)
    }
    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async { Err(ModelsError::new(ModelsErrorCode::Provider)) })
    }
}
pub use resource::OpenAICodexWebSocketDebugStats;
impl OpenAICodexResponsesApi {
    pub fn get_openai_codex_websocket_debug_stats(
        &self,
        session: &str,
    ) -> Option<OpenAICodexWebSocketDebugStats> {
        self.resource.stats(session)
    }
    pub fn reset_openai_codex_websocket_debug_stats(&self, session: Option<&str>) {
        self.resource.reset(session)
    }
    pub fn close_openai_codex_websocket_sessions(
        &self,
        session: Option<&str>,
        runtime: RequestRuntime,
    ) -> BoxFuture<'static, Result<(), TransportError>> {
        self.resource.close(session, runtime)
    }
    pub fn abort_websocket_sessions(&self, session: Option<&str>) {
        self.resource.abort(session)
    }
    async fn run(
        &self,
        r: &TextStreamRequest,
        o: &OpenAICodexResponsesOptions,
        p: &AssistantStreamProducer,
        output: &mut AssistantMessage,
    ) -> Result<(), Failure> {
        let runtime = &r.options.runtime;
        let account = request::account(r, self.services.limits.max_key_bytes)?;
        let account_key = account.key(|| self.resource.fresh_account())?;
        let grammar = create_grammar_tool_input_properties(
            r.context.tools.as_deref(),
            request::compat(&r.resolved.model, "supportsOpenAIGrammarTools", false),
        )
        .map_err(|_| Failure::setup("grammar"))?;
        let session = if r.options.wire.cache_retention == Some(CacheRetention::None) {
            None
        } else {
            r.options
                .wire
                .session_id
                .as_deref()
                .filter(|s| !s.is_empty())
        };
        let mut payload = request::body(r, o, session, &grammar)?;
        if let Some(middleware) = &self.services.adapter.payload
            && let Some(next) = cancellable(
                middleware.on_payload(r, &mut payload),
                &runtime.cancellation,
            )
            .await
            .map_err(|_| Failure::Transport(TransportErrorKind::Cancelled))?
            .map_err(|_| Failure::setup("payload_middleware"))?
        {
            payload = next;
        }
        if let Some(handler) = &self.services.adapter.request_payload
            && let Some(replacement) = cancellable(
                super::request_lifecycle::transform(handler.as_ref(), r, &mut payload),
                &runtime.cancellation,
            )
            .await
            .map_err(|_| Failure::Transport(TransportErrorKind::Cancelled))?
            .map_err(|_| Failure::setup("payload_middleware"))?
        {
            payload = replacement;
        }
        let body = payload.into_value();
        let json = stringify_wire_json(&body).map_err(|_| Failure::Limit)?;
        let affinity = clamp_openai_prompt_cache_key(session);
        let request_id = match affinity.filter(|s| !s.is_empty()) {
            Some(id) => id.to_owned(),
            None => self
                .services
                .request_ids
                .generate()
                .map_err(|_| Failure::setup("request_id"))?,
        };
        let endpoint = request::endpoint(r.resolved.endpoint.expose_for_transport());
        let account_header = account.header()?;
        let mut sse_headers = request::headers(r, &account_header, affinity, false)?;
        let ws_headers = request::headers(r, &account_header, Some(&request_id), true)?;
        let transport = r.options.wire.transport.unwrap_or(Transport::Auto);
        let mut start = false;
        if transport != Transport::Sse {
            if self.resource.sticky(session) {
                self.resource.fallback(session)?;
            } else {
                let mut retried_limit = false;
                let mut retried_previous = false;
                loop {
                    let mut attempt_started = false;
                    match self
                        .websocket(
                            r,
                            o,
                            p,
                            output,
                            &body,
                            &endpoint,
                            &ws_headers,
                            &account_key,
                            session,
                            &grammar,
                            &mut attempt_started,
                            &mut start,
                        )
                        .await
                    {
                        Ok(()) => {
                            self.done(r, p, output)?;
                            return Ok(());
                        }
                        Err(error) => {
                            if runtime.cancellation.is_cancelled() || self.resource.retired() {
                                return Err(error);
                            }
                            let limit = !attempt_started
                                && matches!(error, Failure::Api(ApiCode::ConnectionLimit, _));
                            if matches!(error, Failure::Api(ApiCode::PreviousMissing, _))
                                && !retried_previous
                            {
                                retried_previous = true;
                                continue;
                            }
                            if limit && !retried_limit {
                                retried_limit = true;
                                continue;
                            }
                            if error.non_transport() && !limit {
                                return Err(error);
                            }
                            let mut details = json!({"configuredTransport":transport,"eventsEmitted":attempt_started,"phase":if attempt_started{"after_message_stream_start"}else{"before_message_stream_start"},"requestBytes":json.len()});
                            if !attempt_started {
                                details["fallbackTransport"] = json!("sse");
                            }
                            let details =
                                serde_json::from_value(details).map_err(|_| Failure::Limit)?;
                            output.diagnostics.get_or_insert_default().push(
                                AssistantMessageDiagnostic {
                                    diagnostic_type: DiagnosticType::new(
                                        "provider_transport_failure",
                                    )
                                    .unwrap(),
                                    timestamp: SafeUnsigned::new(runtime.clock.now_unix_ms())
                                        .unwrap_or(SafeUnsigned::MAX),
                                    error: Some(error.diagnostic()),
                                    details: Some(details),
                                },
                            );
                            let _ = p.publish_cancellation_snapshot(output.clone());
                            self.resource.failure(session)?;
                            if attempt_started {
                                return Err(error);
                            }
                            self.resource.fallback(session)?;
                            break;
                        }
                    }
                }
            }
        }
        let encoded = self
            .services
            .compression
            .and_then(|limits| compress_zstd_request(json.as_bytes(), limits).ok());
        if encoded.is_some() {
            sse_headers.set("content-encoding", "zstd")?;
        }
        let http = FetchRequest {
            endpoint: RequestEndpoint::new_resolved(endpoint)
                .map_err(|_| Failure::setup("endpoint"))?,
            method: "POST".into(),
            headers: FetchHeaders::Constructed(sse_headers),
            body: encoded.map_or_else(|| FetchBody::Text(json), FetchBody::Bytes),
            proxy: None,
        };
        let mut response = self.http(r, &http, output).await?;
        if response.body.presence() == ResponseBodyPresence::Absent {
            response.body.close();
            return Err(Failure::setup("missing_body"));
        }
        if !start {
            let _ = p.push(AssistantMessageEvent::Start {
                partial: Box::new(output.clone()),
            });
        }
        let mut source = response::Source {
            input: response::Input::Http {
                body: response.body,
                decoder: Box::new(SseDecoder::new(
                    SseProfile::Codex,
                    self.services.adapter.sse_limits,
                )),
                eof: false,
            },
            runtime: runtime.clone(),
            template: output.clone(),
            producer: p.clone(),
            started: true,
            emit_start: false,
            websocket: false,
            finished: false,
            failure: None,
            idle: None,
            max_message: self.services.limits.max_message_bytes,
        };
        let pricing = response::Pricing(&r.resolved.model);
        let result = process_responses_stream(
            &mut source,
            output,
            p,
            &r.resolved.model,
            &OpenAIResponsesStreamOptions {
                preserve_end_turn: true,
                service_tier: o.service_tier.clone(),
                grammar_tool_input_properties: Some(&grammar),
                service_tier_policy: Some(&pricing),
            },
            &runtime.cancellation,
        )
        .await;
        if let Err(error) = result {
            return Err(source.failure.take().unwrap_or(Failure::Reducer(error)));
        }
        self.done(r, p, output)
    }
    fn done(
        &self,
        r: &TextStreamRequest,
        p: &AssistantStreamProducer,
        output: &AssistantMessage,
    ) -> Result<(), Failure> {
        if r.options.runtime.cancellation.is_cancelled() {
            return Err(Failure::Transport(TransportErrorKind::Cancelled));
        }
        let reason = match output.stop_reason {
            StopReason::Stop => DoneReason::Stop,
            StopReason::Length => DoneReason::Length,
            StopReason::ToolUse => DoneReason::ToolUse,
            _ => return Err(Failure::setup("missing_stop")),
        };
        p.push(AssistantMessageEvent::Done {
            reason,
            message: Box::new(output.clone()),
        })
        .map_err(|_| Failure::setup("settled"))
    }
    async fn http(
        &self,
        r: &TextStreamRequest,
        http: &FetchRequest,
        template: &AssistantMessage,
    ) -> Result<HttpResponse, Failure> {
        let runtime = &r.options.runtime;
        let max = r.options.wire.max_retries.map_or(0, SafeUnsigned::get);
        let policy = CodexRetryOptions {
            max_retries: max,
            max_retry_delay_ms: r.options.wire.max_retry_delay_ms.map(SafeUnsigned::get),
        };
        let timeout = r
            .options
            .wire
            .timeout_ms
            .filter(|n| n.get() > 0)
            .map(|n| Duration::from_millis(n.get()));
        let profile = match (timeout, self.services.signal_presence) {
            (None, _) => TimeoutProfile::None,
            (Some(d), CodexSourceSignalPresence::Present) => TimeoutProfile::Headers(d),
            (Some(d), CodexSourceSignalPresence::Absent) => TimeoutProfile::WholeResponse(d),
        };
        retry_codex_request(runtime, policy, |index| {
            Box::pin(async move {
                let response = self
                    .services
                    .adapter
                    .transport
                    .send_fetch_request(http, runtime, RetryProfile::None, profile)
                    .await;
                let mut response = match response {
                    Ok(response) => response,
                    Err(error) => {
                        return Err(CodexAttemptFailure {
                            error: Failure::from(error),
                            retry: CodexRetryObservation::Backoff,
                        });
                    }
                };
                if let Some(hook) = &self.services.adapter.response {
                    let head = AdapterResponseHead::from_http(response.status, &response.headers)
                        .map_err(|e| CodexAttemptFailure {
                        error: Failure::from(e),
                        retry: CodexRetryObservation::Backoff,
                    })?;
                    if !cancellable(hook.on_response(r, &head), &runtime.cancellation)
                        .await
                        .is_ok_and(|r| r.is_ok())
                    {
                        response.body.close();
                        return Err(CodexAttemptFailure {
                            error: Failure::setup("response_middleware"),
                            retry: CodexRetryObservation::Backoff,
                        });
                    }
                    super::request_lifecycle::record(&self.services.adapter, &head);
                } else {
                    super::request_lifecycle::observe_only(
                        &self.services.adapter,
                        response.status,
                        &response.headers,
                    );
                }
                if (200..300).contains(&response.status) {
                    return Ok(response);
                }
                let bytes = read_body_limited(
                    response.body.as_mut(),
                    runtime,
                    self.services.adapter.max_error_body_bytes,
                )
                .await
                .map_err(|e| CodexAttemptFailure {
                    retry: if e.kind() == TransportErrorKind::LimitExceeded {
                        CodexRetryObservation::Terminal
                    } else {
                        CodexRetryObservation::Backoff
                    },
                    error: Failure::from(e),
                })?;
                let decoded = String::from_utf8_lossy(&bytes);
                let raw = decoded.strip_prefix('\u{feff}').unwrap_or(&decoded);
                let direct = direct_retryable(response.status, raw);
                if index < max && direct {
                    return Err(CodexAttemptFailure {
                        error: Failure::Http(classify(raw, template)),
                        retry: codex_retry_after_delay_ms(
                            &response.headers,
                            runtime.clock.now_unix_ms(),
                            self.services.date_parser.as_ref(),
                        )
                        .map_or(
                            CodexRetryObservation::Backoff,
                            CodexRetryObservation::ServerDelay,
                        ),
                    });
                }
                let formatted = format_http_error(
                    response.status,
                    &String::from_utf8_lossy(
                        &response
                            .status_text
                            .chars()
                            .map(|c| c as u8)
                            .collect::<Vec<_>>(),
                    ),
                    raw,
                    runtime.clock.now_unix_ms(),
                );
                let retry =
                    if formatted.contains("usage limit") || formatted == "Request was aborted" {
                        CodexRetryObservation::Terminal
                    } else {
                        CodexRetryObservation::Backoff
                    };
                Err(CodexAttemptFailure {
                    error: Failure::Http(classify(&formatted, template)),
                    retry,
                })
            })
        })
        .await
        .map_err(|error| match error {
            CodexRetryError::Attempt(error) => error,
            CodexRetryError::Cancelled => Failure::Transport(TransportErrorKind::Cancelled),
            CodexRetryError::InvalidPolicy => Failure::setup("retry_policy"),
            CodexRetryError::DelayExceeded {
                requested_ms,
                limit_ms,
            } => Failure::Delay {
                requested: requested_ms,
                limit: limit_ms,
            },
        })
    }
}
fn direct_retryable(status: u16, text: &str) -> bool {
    let lower = text.to_lowercase();
    if status == 429
        && [
            "gousagelimiterror",
            "freeusagelimiterror",
            "monthly usage limit reached",
            "available balance",
            "insufficient_quota",
            "out of budget",
            "quota exceeded",
            "billing",
        ]
        .iter()
        .any(|s| lower.contains(s))
    {
        return false;
    }
    matches!(status, 429 | 500 | 502 | 503 | 504)
        || ["rate", "service", "upstream", "connection"]
            .iter()
            .zip(["limit", "unavailable", "connect", "refused"])
            .any(|(a, b)| {
                lower.match_indices(a).any(|(i, _)| {
                    let tail = &lower[i + a.len()..];
                    tail.starts_with(b)
                        || tail.chars().next().is_some_and(|c| {
                            // Pinned non-unicode RegExp dot consumes one
                            // UTF-16 unit and excludes all four line endings.
                            c.len_utf16() == 1
                                && !matches!(c, '\n' | '\r' | '\u{2028}' | '\u{2029}')
                                && tail[c.len_utf8()..].starts_with(b)
                        })
                })
            })
        || lower.contains("overloaded")
}
fn format_http_error(status: u16, status_text: &str, raw: &str, now: u64) -> String {
    let mut message = if raw.is_empty() {
        if status_text.is_empty() {
            "Request failed".into()
        } else {
            status_text.to_owned()
        }
    } else {
        raw.to_owned()
    };
    if let Ok(parsed) = serde_json::from_str::<Value>(raw) {
        let error = &parsed["error"];
        if error.is_object() {
            let code = error["code"]
                .as_str()
                .filter(|s| !s.is_empty())
                .or_else(|| error["type"].as_str())
                .unwrap_or("")
                .to_lowercase();
            if status == 429
                || [
                    "usage_limit_reached",
                    "usage_not_included",
                    "rate_limit_exceeded",
                ]
                .iter()
                .any(|s| code.contains(s))
            {
                let plan = error["plan_type"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .map(|s| format!(" ({} plan)", s.to_lowercase()))
                    .unwrap_or_default();
                let reset = error["resets_at"]
                    .as_f64()
                    .filter(|v| *v != 0.0)
                    .map(|v| {
                        format!(
                            " Try again in ~{} min.",
                            ((v * 1000.0 - now as f64) / 60000.0 + 0.5).floor().max(0.0)
                        )
                    })
                    .unwrap_or_default();
                return format!("You have hit your ChatGPT usage limit{plan}.{reset}");
            }
            if let Some(text) = error["message"].as_str().filter(|s| !s.is_empty()) {
                message = text.to_owned();
            }
        }
    }
    message
}

impl OpenAICodexResponsesApi {
    #[allow(clippy::too_many_arguments)] // One borrowed request attempt, no retained duplicate request owner.
    async fn websocket(
        &self,
        r: &TextStreamRequest,
        o: &OpenAICodexResponsesOptions,
        p: &AssistantStreamProducer,
        output: &mut AssistantMessage,
        body: &Value,
        endpoint: &str,
        headers: &HttpHeaders,
        account: &str,
        session: Option<&str>,
        grammar: &BTreeMap<String, String>,
        started: &mut bool,
        start_emitted: &mut bool,
    ) -> Result<(), Failure> {
        let connector = self
            .services
            .websocket
            .as_ref()
            .ok_or(Failure::Transport(TransportErrorKind::Connection))?;
        let mut url = url::Url::parse(endpoint).map_err(|_| Failure::setup("endpoint"))?;
        if url.scheme() == "https" {
            url.set_scheme("wss")
                .map_err(|_| Failure::setup("endpoint"))?;
        } else if url.scheme() == "http" {
            url.set_scheme("ws")
                .map_err(|_| Failure::setup("endpoint"))?;
        }
        let request = WebSocketRequest {
            endpoint: RequestEndpoint::new_resolved(url.as_str())
                .map_err(|_| Failure::setup("endpoint"))?,
            headers: headers.clone(),
            proxy: None,
            connect_timeout: Some(Duration::from_millis(
                r.options
                    .wire
                    .websocket_connect_timeout_ms
                    .map_or(15000, SafeUnsigned::get),
            )),
            max_message_bytes: self.services.limits.max_message_bytes,
            max_frame_bytes: self.services.limits.max_message_bytes,
        };
        let mut lease = self
            .resource
            .acquire(
                session,
                account,
                connector.as_ref(),
                &request,
                &r.options.runtime,
            )
            .await?;
        let cached = matches!(
            r.options.wire.transport,
            Some(Transport::Auto | Transport::WebSocketCached)
        );
        let mut selected = body.clone();
        if cached
            && lease.cached()
            && let Some(previous) = &lease.continuation
        {
            if let Some(delta) = input_delta(body, previous) {
                if !selected.is_object() {
                    selected = spread_value(&selected);
                }
                selected["previous_response_id"] = json!(previous.id);
                selected["input"] = json!(delta);
            } else {
                lease.continuation = None;
            }
        }
        self.resource
            .request_stats(session, lease.reused, cached, &selected)?;
        if selected.is_null() && session.is_some() {
            return Err(Failure::setup("payload_null"));
        }
        let result = async {
            let json = if let Some(text) = body.as_str() {
                string_spread_json(
                    text,
                    true,
                    selected.get("previous_response_id"),
                    selected.get("input"),
                )?
            } else {
                let mut send = json!({"type":"response.create"});
                for (k, v) in spread_value(&selected).as_object().expect("spread object") {
                    send[k] = v.clone();
                }
                stringify_wire_json(&send).map_err(|_| Failure::Limit)?
            };
            let exchange = lease.start(json, &r.options.runtime).await?;
            let mut source = response::Source {
                input: response::Input::Socket(exchange),
                runtime: lease.runtime(&r.options.runtime),
                template: output.clone(),
                producer: p.clone(),
                started: false,
                emit_start: !*start_emitted,
                websocket: true,
                finished: false,
                failure: None,
                idle: r
                    .options
                    .wire
                    .timeout_ms
                    .filter(|n| n.get() > 0)
                    .map(|n| Duration::from_millis(n.get())),
                max_message: self.services.limits.max_message_bytes,
            };
            let pricing = response::Pricing(&r.resolved.model);
            let result = process_responses_stream(
                &mut source,
                output,
                p,
                &r.resolved.model,
                &OpenAIResponsesStreamOptions {
                    preserve_end_turn: true,
                    service_tier: o.service_tier.clone(),
                    grammar_tool_input_properties: Some(grammar),
                    service_tier_policy: Some(&pricing),
                },
                &r.options.runtime.cancellation,
            )
            .await;
            *started = source.started;
            *start_emitted |= source.started;
            if let Err(error) = result {
                return Err(source.failure.take().unwrap_or(Failure::Reducer(error)));
            }
            if r.options.runtime.cancellation.is_cancelled() {
                return Err(Failure::Transport(TransportErrorKind::Cancelled));
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            lease.close(&r.options.runtime).await;
            return result;
        }
        let continuation = if cached && lease.cached() {
            if let Some(id) = &output.response_id {
                let context = Context {
                    system_prompt: None,
                    messages: vec![Message::Assistant(Box::new(output.clone()))],
                    tools: None,
                };
                let items = convert_responses_messages(
                    &r.resolved.model,
                    &context,
                    &BTreeSet::from(["openai".into(), "openai-codex".into(), "opencode".into()]),
                    &ConvertResponsesMessagesOptions {
                        include_system_prompt: Some(false),
                        grammar_tool_input_properties: Some(grammar),
                        ..Default::default()
                    },
                )?
                .into_iter()
                .filter(|item| {
                    !matches!(
                        item["type"].as_str(),
                        Some("function_call_output" | "custom_tool_call_output")
                    )
                })
                .collect::<Vec<_>>();
                let bytes = stringify_wire_json(&json!({"body":body,"items":items,"id":id}))
                    .map_err(|_| Failure::Limit)?;
                if bytes.len() > self.services.limits.max_continuation_bytes {
                    return Err(Failure::Limit);
                }
                Some(Continuation {
                    body: body.clone(),
                    id: id.as_str().to_owned(),
                    items,
                })
            } else {
                lease.continuation.take()
            }
        } else {
            lease.continuation.take()
        };
        if lease.cached() {
            lease.retain(continuation, &r.options.runtime)?;
        } else {
            lease.close(&r.options.runtime).await;
        }
        Ok(())
    }
}
fn input_delta(body: &Value, previous: &Continuation) -> Option<Vec<Value>> {
    fn without_input(body: &Value) -> Option<String> {
        if body.is_null() {
            return None;
        }
        if let Some(text) = body.as_str() {
            return string_spread_json(text, false, None, None).ok();
        }
        let mut map = serde_json::Map::new();
        for (k, v) in spread_value(body).as_object()? {
            if k != "input" && k != "previous_response_id" {
                map.insert(k.clone(), v.clone());
            }
        }
        stringify_wire_json(&Value::Object(map)).ok()
    }
    if without_input(body)? != without_input(&previous.body)? {
        return None;
    }
    let empty = Vec::new();
    let current = body["input"].as_array().unwrap_or(&empty);
    let mut baseline = previous.body["input"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    baseline.extend(previous.items.clone());
    if current.len() < baseline.len()
        || stringify_wire_json(&json!(&current[..baseline.len()])).ok()?
            != stringify_wire_json(&json!(baseline)).ok()?
    {
        return None;
    }
    Some(current[baseline.len()..].to_vec())
}

fn spread_value(value: &Value) -> Value {
    match value {
        Value::Object(_) => value.clone(),
        Value::Array(values) => Value::Object(
            values
                .iter()
                .enumerate()
                .map(|(i, v)| (i.to_string(), v.clone()))
                .collect(),
        ),
        _ => json!({}),
    }
}
// String object spread enumerates UTF-16 code units, including separately escaped
// surrogate halves. Keep those generated wire strings out of scalar-only Value.
fn string_spread_json(
    text: &str,
    with_type: bool,
    previous: Option<&Value>,
    input: Option<&Value>,
) -> Result<String, Failure> {
    let mut out = String::from("{");
    let mut first = true;
    let mut field = |key: &str, value: &str| -> Result<(), Failure> {
        if out
            .len()
            .saturating_add(key.len())
            .saturating_add(value.len())
            .saturating_add(5)
            > 1_048_576
        {
            return Err(Failure::Limit);
        }
        if !first {
            out.push(',');
        }
        first = false;
        out.push('"');
        out.push_str(key);
        out.push_str("\":");
        out.push_str(value);
        Ok(())
    };
    for (i, unit) in text.encode_utf16().enumerate() {
        let value = if (0xd800..=0xdfff).contains(&unit) {
            format!("\"\\u{unit:04x}\"")
        } else {
            stringify_wire_json(&json!(
                char::from_u32(u32::from(unit))
                    .expect("non-surrogate unit")
                    .to_string()
            ))
            .map_err(|_| Failure::Limit)?
        };
        field(&i.to_string(), &value)?;
    }
    if with_type {
        field("type", "\"response.create\"")?;
    }
    if let Some(previous) = previous {
        field(
            "previous_response_id",
            &stringify_wire_json(previous).map_err(|_| Failure::Limit)?,
        )?;
    }
    if let Some(input) = input {
        field(
            "input",
            &stringify_wire_json(input).map_err(|_| Failure::Limit)?,
        )?;
    }
    out.push('}');
    Ok(out)
}

impl pi_ai_core::RequestLifecycleAdapter for OpenAICodexResponsesApi {
    fn stream_with_lifecycle(
        &self,
        request: TextStreamRequest,
        lifecycle: pi_ai_core::RequestLifecycleInvocation,
    ) -> AssistantStream {
        // Retain the existing shared ResourceOwner across this scoped binding.
        let mut scoped = self.clone();
        super::request_lifecycle::bind(&mut scoped.services.adapter, lifecycle);
        scoped.stream(request)
    }
    fn stream_simple_with_lifecycle(
        &self,
        request: SimpleStreamRequest,
        lifecycle: pi_ai_core::RequestLifecycleInvocation,
    ) -> AssistantStream {
        // Retain the existing shared ResourceOwner across this scoped binding.
        let mut scoped = self.clone();
        super::request_lifecycle::bind(&mut scoped.services.adapter, lifecycle);
        scoped.stream_simple(request)
    }
}
