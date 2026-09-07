use super::{
    CompletionsError, CompletionsToolChoice, CompletionsToolChoiceMode, OpenAICompletionsOptions,
    ResolvedCompletionsCompat, payload, reduce::Reducer,
};
use crate::api::{
    constrained_sampling::create_grammar_tool_input_properties,
    github_copilot_headers::{build_copilot_dynamic_headers, has_copilot_vision_input},
    openai_sdk_strings,
    runtime::{AdapterResponseHead, AdapterServices, cancellable},
    simple_options::build_base_options,
    wire_json::stringify_wire_json,
};
use pi_ai_core::{
    ApiAdapter, ApiId, AssistantMessage, AssistantMessageDiagnostic, AssistantMessageEvent,
    AssistantRole, AssistantStream, AssistantStreamInit, AssistantStreamProducer, BoxFuture,
    CacheRetention, CapabilityError, DeferredCancelRequest, DeferredFetchRequest, DoneReason,
    ErrorReason, ModelThinkingLevel, ModelsError, ModelsErrorCode, RequestEndpoint, SafeUnsigned,
    SimpleStreamRequest, StopReason, StreamOptions, StreamSetupFailure, TextStreamRequest,
    ThinkingLevel, ToolChoice, Usage, clamp_thinking_level, create_assistant_message_event_stream,
    is_context_overflow, is_retryable_assistant_error,
};
use pi_ai_transport::{
    HttpHeaders, HttpRequest, HttpResponse, ProviderAttemptFailure, ProviderRetryError,
    RetryProfile, SseDecoder, SseProfile, TimeoutProfile, TransportError, TransportErrorKind,
    read_body_limited,
};
use serde_json::{Value, json};
use std::time::Duration;
use url::Url;

#[derive(Clone, Debug)]
pub struct OpenAICompletionsApi {
    api: ApiId,
    pub services: AdapterServices,
    pub defaults: OpenAICompletionsOptions,
    pub max_stream_bytes: usize,
}
impl OpenAICompletionsApi {
    pub fn new(services: AdapterServices) -> Self {
        Self {
            api: ApiId::new("openai-completions").expect("constant"),
            services,
            defaults: OpenAICompletionsOptions::default(),
            max_stream_bytes: 16 * 1024 * 1024,
        }
    }
    pub fn stream_with_options(
        &self,
        request: TextStreamRequest,
        options: OpenAICompletionsOptions,
    ) -> AssistantStream {
        self.start(request, options, false)
    }
    fn start(
        &self,
        mut request: TextStreamRequest,
        options: OpenAICompletionsOptions,
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
        if request.resolved.model.api != self.api {
            fail(&request, &producer, output, Failure::plain("api_mismatch"));
            return stream;
        }
        let services = self.services.clone();
        let limit = self.max_stream_bytes;
        let worker = producer.clone();
        let spawner = request.options.runtime.spawner.clone();
        let task = Box::pin(async move {
            let result = async {
                if simple {
                    api_key(&request)?;
                    request.options.wire = build_base_options(
                        &request.resolved.model,
                        &request.context,
                        &request.options.wire,
                    )
                    .map_err(|_| Failure::plain("context_estimate"))?;
                }
                run(&request, &options, &services, limit, &worker, &mut output).await
            }
            .await;
            if let Err(error) = result {
                fail(&request, &worker, output, error);
            }
        });
        if spawner.spawn(task).is_err() {
            let _ = producer.fail_setup(StreamSetupFailure::Spawn);
        }
        stream
    }
}
impl ApiAdapter for OpenAICompletionsApi {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        self.stream_with_options(request, self.defaults.clone())
    }
    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        let reasoning_effort = request.options.wire.reasoning.and_then(|level| {
            match clamp_thinking_level(&request.resolved.model, payload::model_level(level)) {
                ModelThinkingLevel::Off => None,
                ModelThinkingLevel::Minimal => Some(ThinkingLevel::Minimal),
                ModelThinkingLevel::Low => Some(ThinkingLevel::Low),
                ModelThinkingLevel::Medium => Some(ThinkingLevel::Medium),
                ModelThinkingLevel::High => Some(ThinkingLevel::High),
                ModelThinkingLevel::ExtraHigh => Some(ThinkingLevel::ExtraHigh),
                ModelThinkingLevel::Max => Some(ThinkingLevel::Max),
            }
        });
        let options = OpenAICompletionsOptions {
            reasoning_effort,
            thinking_budgets: request.options.wire.thinking_budgets,
            tool_choice: request.options.wire.tool_choice.map(|c| {
                CompletionsToolChoice::Mode(match c {
                    ToolChoice::Auto => CompletionsToolChoiceMode::Auto,
                    ToolChoice::None => CompletionsToolChoiceMode::None,
                })
            }),
        };
        self.start(
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
            true,
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
        timestamp: SafeUnsigned::new(request.options.runtime.clock.now_unix_ms())
            .unwrap_or(SafeUnsigned::MAX),
    }
}
fn whitespace(c: char) -> bool {
    matches!(c,'\u{9}'..='\u{d}'|' '| '\u{a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}'|'\u{feff}')
}
fn api_key(request: &TextStreamRequest) -> Result<&str, Failure> {
    if let Some(key) = request
        .auth
        .api_key
        .as_ref()
        .map(|v| v.expose_secret())
        .filter(|v| !v.is_empty())
    {
        return Ok(key);
    }
    if request.auth.headers.as_map().iter().any(|(k, v)| {
        (k.eq_ignore_ascii_case("authorization") || k.eq_ignore_ascii_case("cf-aig-authorization"))
            && v.as_ref()
                .is_some_and(|v| !v.trim_matches(whitespace).is_empty())
    }) {
        return Ok("unused");
    }
    Err(Failure::plain("authentication"))
}
fn retention(request: &TextStreamRequest, services: &AdapterServices) -> CacheRetention {
    request.options.wire.cache_retention.unwrap_or_else(|| {
        if request
            .auth
            .env
            .as_map()
            .get("PI_CACHE_RETENTION")
            .filter(|s| !s.is_empty())
            .or_else(|| services.env.as_map().get("PI_CACHE_RETENTION"))
            .is_some_and(|s| s == "long")
        {
            CacheRetention::Long
        } else {
            CacheRetention::Short
        }
    })
}
fn set(headers: &mut HttpHeaders, key: &str, value: &str) -> Result<(), Failure> {
    if value.chars().any(|c| u32::from(c) > 255) {
        return Err(Failure::plain("header"));
    }
    headers.set(key, value.trim_matches([' ', '\t', '\r', '\n']))?;
    Ok(())
}
fn headers(
    request: &TextStreamRequest,
    services: &AdapterServices,
    key: Option<&str>,
    cache: CacheRetention,
    compat: &ResolvedCompletionsCompat,
    has_body: bool,
) -> Result<HttpHeaders, Failure> {
    let mut h = HttpHeaders::new();
    // SDK construction validates defaultHeaders only. Auth/org/project and
    // encoder headers belong to the later request build, after onPayload.
    if let Some(key) = key {
        for (k, v) in [
            ("accept", "application/json"),
            ("user-agent", "pi-ai-rust"),
            ("x-stainless-retry-count", "0"),
        ] {
            set(&mut h, k, v)?;
        }
        if let Some(ms) = request.options.wire.timeout_ms.filter(|v| v.get() != 0) {
            set(
                &mut h,
                "x-stainless-timeout",
                &(ms.get() / 1000).to_string(),
            )?;
        }
        for (env, k) in [
            ("OPENAI_ORG_ID", "openai-organization"),
            ("OPENAI_PROJECT_ID", "openai-project"),
        ] {
            if let Some(v) = openai_sdk_strings::read_env(&services.env, env) {
                set(&mut h, k, v)?;
            }
        }
        set(&mut h, "authorization", &format!("Bearer {key}"))?;
    }
    if let Some(custom) = openai_sdk_strings::read_env(&services.env, "OPENAI_CUSTOM_HEADERS") {
        for (k, v) in openai_sdk_strings::custom_headers(custom) {
            set(&mut h, k, v)?;
        }
    }
    set(&mut h, "user-agent", "pi-ai-rust")?;
    if let Some(model) = &request.resolved.model.headers {
        for (k, v) in model.as_map() {
            set(&mut h, k, v)?;
        }
    }
    if request.resolved.model.provider.as_str() == "github-copilot" {
        for (k, v) in build_copilot_dynamic_headers(
            &request.context.messages,
            has_copilot_vision_input(&request.context.messages),
        ) {
            set(&mut h, &k, &v)?;
        }
    }
    if cache != CacheRetention::None
        && compat.flag("sendSessionAffinityHeaders")
        && let Some(session) = request
            .options
            .wire
            .session_id
            .as_deref()
            .filter(|s| !s.is_empty())
    {
        if compat.text("sessionAffinityFormat") == "openrouter" {
            set(&mut h, "x-session-id", session)?;
        } else {
            if compat.text("sessionAffinityFormat") == "openai" {
                set(&mut h, "session_id", session)?;
            }
            set(&mut h, "x-client-request-id", session)?;
            set(&mut h, "x-session-affinity", session)?;
        }
    }
    for (k, v) in request.auth.headers.iter_for_dispatch() {
        match v {
            Some(v) => set(&mut h, k, v)?,
            None => {
                let mut checked = HttpHeaders::new();
                checked.set(k, "")?;
                h.remove(k);
            }
        }
    }
    if has_body {
        set(&mut h, "content-type", "application/json")?;
    }
    Ok(h)
}
fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::String(s) => !s.is_empty(),
        Value::Number(n) => n.as_f64() != Some(0.0),
        _ => true,
    }
}
fn endpoint(base: &str) -> Result<RequestEndpoint, Failure> {
    let mut url = Url::parse(&format!(
        "{base}{}",
        if base.ends_with('/') {
            "chat/completions"
        } else {
            "/chat/completions"
        }
    ))
    .map_err(|_| Failure::plain("endpoint"))?;
    let mut pairs: Vec<(String, String)> = Vec::new();
    for (k, v) in url.query_pairs() {
        if let Some((_, old)) = pairs.iter_mut().find(|(old, _)| old == &k) {
            *old = v.into_owned();
        } else {
            pairs.push((k.into_owned(), v.into_owned()));
        }
    }
    if !pairs.is_empty() {
        pairs.sort_by_key(|(k, _)| {
            k.parse::<u32>()
                .ok()
                .filter(|v| *v != u32::MAX && v.to_string() == *k)
                .map_or((1, 0), |v| (0, v))
        });
        let encode = |s: &str| {
            s.bytes()
                .map(|b| {
                    if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                        char::from(b).to_string()
                    } else {
                        format!("%{b:02X}")
                    }
                })
                .collect::<String>()
        };
        url.set_query(Some(
            &pairs
                .iter()
                .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
                .collect::<Vec<_>>()
                .join("&"),
        ));
    }
    RequestEndpoint::new_resolved(url.as_str()).map_err(|_| Failure::plain("endpoint"))
}
fn attempt<'a>(
    http: &'a HttpRequest,
    request: &'a TextStreamRequest,
    services: &'a AdapterServices,
    output: &'a AssistantMessage,
) -> BoxFuture<'a, Result<HttpResponse, ProviderAttemptFailure<Failure>>> {
    Box::pin(async move {
        let mut response = services
            .transport
            .send_fetch(
                http,
                &request.options.runtime,
                RetryProfile::None,
                TimeoutProfile::Headers(Duration::from_millis(
                    request
                        .options
                        .wire
                        .timeout_ms
                        .map(|v| v.get())
                        .unwrap_or(600000),
                )),
            )
            .await
            .map_err(|e| {
                let kind = e.kind();
                let error = Failure::from(e);
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
            return Ok(response);
        }
        let bytes = match read_body_limited(
            response.body.as_mut(),
            &request.options.runtime,
            services.max_error_body_bytes,
        )
        .await
        {
            Ok(bytes) => bytes,
            Err(e)
                if matches!(
                    e.kind(),
                    TransportErrorKind::Cancelled | TransportErrorKind::LimitExceeded
                ) =>
            {
                return Err(ProviderAttemptFailure::Other(Failure::from(e)));
            }
            Err(e) => {
                if e.kind() == TransportErrorKind::Timeout {
                    b"Request timed out.".to_vec()
                } else {
                    b"terminated".to_vec()
                }
            }
        };
        Err(ProviderAttemptFailure::Provider {
            status: Some(response.status),
            headers: Some(response.headers.clone()),
            error: sdk_failure(Some(response.status), &bytes, output),
        })
    })
}
async fn run(
    request: &TextStreamRequest,
    options: &OpenAICompletionsOptions,
    services: &AdapterServices,
    limit: usize,
    producer: &AssistantStreamProducer,
    output: &mut AssistantMessage,
) -> Result<(), Failure> {
    let key = api_key(request)?;
    let compat =
        ResolvedCompletionsCompat::resolve(&request.resolved.model, &request.resolved.endpoint);
    let grammar = create_grammar_tool_input_properties(
        request.context.tools.as_deref(),
        compat.flag("supportsOpenAIGrammarTools"),
    )
    .map_err(|_| Failure::plain("grammar"))?;
    let cache = retention(request, services);
    let runtime = &request.options.runtime;
    if openai_sdk_strings::read_env(&services.env, "OPENAI_CUSTOM_HEADERS").is_some() {
        headers(request, services, None, cache, &compat, false)?;
    }
    let value = payload::build(request, options, &compat, cache, &grammar)?;
    let mut payload = serde_json::from_value(value).map_err(|_| Failure::plain("payload"))?;
    if let Some(middleware) = &services.payload
        && let Some(next) = cancellable(
            middleware.on_payload(request, &mut payload),
            &runtime.cancellation,
        )
        .await
        .map_err(|_| Failure::plain("cancelled"))?
        .map_err(|_| Failure::plain("payload_middleware"))?
    {
        payload = next;
    }
    let value = serde_json::to_value(payload).map_err(|_| Failure::plain("payload"))?;
    if value.is_null() {
        return Err(Failure::plain("payload"));
    }
    let streaming = value.get("stream").is_some_and(truthy);
    let has_body = truthy(&value);
    let http = HttpRequest {
        endpoint: endpoint(request.resolved.endpoint.expose_for_transport())?,
        method: "POST".to_owned(),
        headers: headers(request, services, Some(key), cache, &compat, has_body)?,
        body: if has_body {
            stringify_wire_json(&value)
                .map_err(|_| Failure::plain("payload_limit"))?
                .into_bytes()
        } else {
            Vec::new()
        },
        proxy: None,
    };
    let mut response = services
        .transport
        .retry_provider_request(
            runtime,
            RetryProfile::SharedProvider {
                max_retries: request
                    .options
                    .wire
                    .max_retries
                    .map(|v| v.get())
                    .unwrap_or(0),
                max_retry_delay_ms: request.options.wire.max_retry_delay_ms.map(|v| v.get()),
            },
            || attempt(&http, request, services, output),
        )
        .await
        .map_err(retry_failure)?;
    let mut buffered = Value::Null;
    if !streaming && response.status != 204 {
        let content_type = response
            .headers
            .get_for_fetch("content-type")
            .unwrap_or_default();
        let media = content_type
            .split(';')
            .next()
            .unwrap_or_default()
            .trim_matches(whitespace);
        let is_json = media.contains("application/json") || media.ends_with("+json");
        // OpenAI6.40.0 internal/parse.ts returns undefined without touching a
        // JSON body whose Fetch-projected Content-Length is exactly "0".
        if !(is_json && response.headers.get_for_fetch("content-length").as_deref() == Some("0")) {
            let bytes = read_body_limited(
                response.body.as_mut(),
                runtime,
                services.max_error_body_bytes,
            )
            .await?;
            let raw = String::from_utf8_lossy(&bytes);
            let raw = raw.strip_prefix('\u{feff}').unwrap_or(&raw);
            buffered = if is_json {
                serde_json::from_str(raw).map_err(|_| Failure::plain("invalid_response"))?
            } else {
                json!(raw)
            };
        }
    }
    if let Some(middleware) = &services.response {
        let head = AdapterResponseHead::from_http(response.status, &response.headers)?;
        cancellable(
            middleware.on_response(request, &head),
            &runtime.cancellation,
        )
        .await
        .map_err(|_| Failure::plain("cancelled"))?
        .map_err(|_| Failure::plain("response_middleware"))?;
    }
    producer
        .publish_cancellation_snapshot(output.clone())
        .map_err(|_| Failure::plain("producer"))?;
    producer
        .push(AssistantMessageEvent::Start {
            partial: Box::new(output.clone()),
        })
        .map_err(|_| Failure::plain("producer"))?;
    let mut reducer = Reducer::new(output);
    let result = async {
        if !streaming {
            if let Some(events) = buffered.as_array() {
                for event in events {
                    reducer.chunk(event, &request.resolved.model, &grammar, producer)?;
                }
            } else if !buffered.is_string() {
                return Err(Failure::plain("non_streaming_response"));
            }
        } else {
            if matches!(response.status, 204 | 205) {
                return Err(Failure::plain("no_body"));
            }
            let mut decoder = SseDecoder::new(SseProfile::OpenAI, services.sse_limits);
            let mut ended = false;
            let mut done = false;
            let mut bytes = 0usize;
            loop {
                while let Some(frame) = decoder.next_frame()? {
                    if done {
                        continue;
                    }
                    if frame.data.starts_with("[DONE]") {
                        done = true;
                        continue;
                    }
                    let chunk: Value = serde_json::from_str(&frame.data)
                        .map_err(|_| Failure::plain("invalid_json"))?;
                    if frame
                        .event
                        .as_deref()
                        .is_some_and(|e| e.starts_with("thread."))
                    {
                        continue;
                    }
                    if chunk.get("error").is_some_and(truthy) {
                        return Err(sdk_failure(None, frame.data.as_bytes(), output));
                    }
                    reducer.chunk(&chunk, &request.resolved.model, &grammar, producer)?;
                    if let Ok(partial) = reducer.safe_partial() {
                        *output = partial;
                    }
                }
                if ended {
                    break;
                }
                match cancellable(response.body.next(runtime), &runtime.cancellation)
                    .await
                    .map_err(|_| Failure::plain("cancelled"))??
                {
                    Some(chunk) => {
                        bytes = bytes
                            .checked_add(chunk.len())
                            .filter(|n| *n <= limit)
                            .ok_or_else(|| Failure::plain("stream_limit"))?;
                        decoder.push(&chunk)?;
                    }
                    None => {
                        ended = true;
                        decoder.end_input();
                    }
                }
            }
        }
        reducer.finish(&compat, producer)?;
        *output = reducer.partial()?;
        if output.stop_reason == StopReason::Error {
            return Err(Failure::classified("finish_reason", output, None));
        }
        let reason = match output.stop_reason {
            StopReason::Stop => DoneReason::Stop,
            StopReason::Length => DoneReason::Length,
            StopReason::ToolUse => DoneReason::ToolUse,
            _ => return Err(Failure::plain("finish_reason")),
        };
        producer
            .push(AssistantMessageEvent::Done {
                reason,
                message: Box::new(output.clone()),
            })
            .map_err(|_| Failure::plain("producer"))?;
        Ok(())
    }
    .await;
    response.body.close();
    if result.is_err()
        && let Ok(partial) = reducer.caught_partial()
    {
        *output = partial;
    }
    result
}

#[derive(Clone, Copy, Default)]
struct Observation {
    retryable: bool,
    overflow: bool,
}
#[derive(Clone, Copy)]
struct Failure {
    kind: &'static str,
    status: Option<u16>,
    normal: Observation,
    cap: Observation,
}
impl Failure {
    fn plain(kind: &'static str) -> Self {
        Self {
            kind,
            status: None,
            normal: Observation::default(),
            cap: Observation::default(),
        }
    }
    fn classified(kind: &'static str, output: &AssistantMessage, status: Option<u16>) -> Self {
        let normal = observe(output);
        Self {
            kind,
            status,
            normal,
            cap: normal,
        }
    }
}
fn observe(output: &AssistantMessage) -> Observation {
    Observation {
        retryable: is_retryable_assistant_error(output),
        overflow: is_context_overflow(output, None),
    }
}
impl From<CompletionsError> for Failure {
    fn from(e: CompletionsError) -> Self {
        let mut f = Self::plain(match e {
            CompletionsError::Conversion => "conversion",
            CompletionsError::InvalidWire => "invalid_wire",
            CompletionsError::ResourceLimit => "resource_limit",
            CompletionsError::MissingFinishReason => "missing_finish_reason",
        });
        f.normal.retryable = e == CompletionsError::MissingFinishReason;
        f.cap = f.normal;
        f
    }
}
impl From<TransportError> for Failure {
    fn from(e: TransportError) -> Self {
        let mut f = Self::plain("transport");
        f.normal.retryable = matches!(
            e.kind(),
            TransportErrorKind::Connection | TransportErrorKind::Timeout
        );
        f.cap = f.normal;
        f
    }
}
fn retry_failure(error: ProviderRetryError<Failure>) -> Failure {
    match error {
        ProviderRetryError::Attempt(
            ProviderAttemptFailure::Provider { error, .. } | ProviderAttemptFailure::Other(error),
        ) => error,
        ProviderRetryError::Cancelled { .. } => Failure::plain("cancelled"),
        ProviderRetryError::Policy { error, previous } => {
            if error.kind() == TransportErrorKind::RetryDelayExceeded
                && let Some(
                    ProviderAttemptFailure::Provider { mut error, .. }
                    | ProviderAttemptFailure::Other(mut error),
                ) = previous
            {
                error.kind = "retry_delay_cap";
                error.normal = error.cap;
                return error;
            }
            Failure::from(error)
        }
    }
}
fn sdk_failure(status: Option<u16>, bytes: &[u8], template: &AssistantMessage) -> Failure {
    let raw = String::from_utf8_lossy(bytes);
    let raw = raw.strip_prefix('\u{feff}').unwrap_or(&raw);
    let parsed = serde_json::from_str::<Value>(raw).ok();
    let error = parsed.as_ref().and_then(|v| v.get("error"));
    let msg = error
        .filter(|v| truthy(v))
        .map(|e| {
            e.get("message")
                .filter(|v| truthy(v))
                .map(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| stringify_wire_json(v).unwrap_or_default())
                })
                .unwrap_or_else(|| stringify_wire_json(e).unwrap_or_default())
        })
        .or_else(|| {
            parsed
                .as_ref()
                .is_none_or(|v| !truthy(v))
                .then(|| raw.to_owned())
        })
        .filter(|s| !s.is_empty());
    let sdk = match (status, msg) {
        (Some(s), Some(m)) => format!("{s} {m}"),
        (Some(s), None) => format!("{s} status code (no body)"),
        (None, Some(m)) => m,
        _ => "(no status code or body)".to_owned(),
    };
    let mut text = sdk.clone();
    if let Some(e) = error.filter(|v| v.as_object().is_some_and(|m| !m.is_empty())) {
        let body = stringify_wire_json(e).unwrap_or_default();
        let body = body.trim_matches(whitespace);
        let units = body.encode_utf16().collect::<Vec<_>>();
        let short = if units.len() > 4000 {
            format!(
                "{}... [truncated {} chars]",
                String::from_utf16_lossy(&units[..4000]),
                units.len() - 4000
            )
        } else {
            body.to_owned()
        };
        if !text.contains(&short)
            && let Some(status) = status
        {
            text = format!("{status}: {short}");
        }
    }
    if let Some(raw) = error
        .and_then(|v| v.get("metadata"))
        .and_then(|v| v.get("raw"))
        .filter(|v| truthy(v))
    {
        let raw = js_string(raw);
        if !text.contains(&raw) {
            text.push('\n');
            text.push_str(&raw);
        }
    }
    let mut probe = template.clone();
    probe.stop_reason = StopReason::Error;
    probe.error_message = Some(text);
    let normal = observe(&probe);
    probe.error_message = Some(format!("Server requested retry delay. {sdk}"));
    let cap = observe(&probe);
    Failure {
        kind: "provider",
        status,
        normal,
        cap,
    }
}

fn js_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Object(_) => "[object Object]".to_owned(),
        Value::Array(values) => values
            .iter()
            .map(|v| {
                if v.is_null() {
                    String::new()
                } else {
                    js_string(v)
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        value => stringify_wire_json(value).unwrap_or_default(),
    }
}
fn fail(
    request: &TextStreamRequest,
    producer: &AssistantStreamProducer,
    mut output: AssistantMessage,
    error: Failure,
) {
    let aborted = request.options.runtime.cancellation.is_cancelled();
    let obs = error.normal;
    output.stop_reason = if aborted {
        StopReason::Aborted
    } else {
        StopReason::Error
    };
    output.error_message = Some(
        if aborted {
            pi_ai_core::REQUEST_CANCELLED_MESSAGE
        } else {
            match (obs.overflow, obs.retryable) {
                (true, true) => "prompt is too long; server error",
                (true, false) => "prompt is too long",
                (false, true) => "server error",
                _ => "OpenAI Completions request failed",
            }
        }
        .to_owned(),
    );
    if !aborted {
        output.diagnostics=Some(vec![AssistantMessageDiagnostic {diagnostic_type:"openai_completions_failure".parse().expect("constant"),timestamp:output.timestamp,error:None,details:Some(serde_json::from_value(json!({"kind":error.kind,"status":error.status,"retryable":obs.retryable,"contextOverflow":obs.overflow})).expect("safe diagnostic"))}]);
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
