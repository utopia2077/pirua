//! Standard OpenAI Responses wrapper, pinned to pi-mono 853a80d and OpenAI 6.40.0.
//! Neutral options, auth and endpoint custody remain on the nominal request.
//! SDK platform identity is normalized to Rust; no environment is read implicitly.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::time::Duration;

use pi_ai_core::{
    ApiAdapter, ApiId, AssistantMessage, AssistantMessageDiagnostic, AssistantMessageEvent,
    AssistantRole, AssistantStream, AssistantStreamInit, AssistantStreamProducer, BoxFuture,
    CacheRetention, CapabilityError, DeferredCancelRequest, DeferredFetchRequest, DoneReason,
    ErrorReason, FiniteF64, JsonObject, JsonValue, Model, ModelThinkingLevel, ModelsError,
    ModelsErrorCode, Presence, RequestEndpoint, RequestRuntime, SafeUnsigned, SimpleStreamRequest,
    StopReason, StreamOptions, StreamSetupFailure, TextStreamRequest, ThinkingLevel, ToolChoice,
    Usage, clamp_thinking_level, create_assistant_message_event_stream, is_context_overflow,
    is_retryable_assistant_error, split_deferred_tools,
};
use pi_ai_transport::{
    HttpHeaders, HttpRequest, HttpResponse, ProviderAttemptFailure, ProviderRetryError,
    ResponseBody, RetryProfile, SseDecoder, SseProfile, TimeoutProfile, TransportError,
    TransportErrorKind, read_body_limited,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;

use super::constrained_sampling::create_grammar_tool_input_properties;
use super::github_copilot_headers::{build_copilot_dynamic_headers, has_copilot_vision_input};
use super::openai_prompt_cache::clamp_openai_prompt_cache_key;
use super::openai_responses_shared::{
    ConvertResponsesMessagesOptions, ConvertResponsesToolsOptions, OpenAIResponsesStreamOptions,
    ResponsesDeferredToolsMode, ResponsesError, ResponsesErrorKind, ResponsesEventSource,
    ResponsesServiceTier, ResponsesServiceTierPolicy, ResponsesServiceTierPricing,
    convert_responses_messages, convert_responses_tools, process_responses_stream,
};
use super::runtime::{AdapterResponseHead, AdapterServices, cancellable};
use super::simple_options::build_base_options;
use super::wire_json::stringify_wire_json;

fn optional_non_null<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)?
        .map(Some)
        .ok_or_else(|| serde::de::Error::custom("expected a non-null optional value"))
}

/// Family fields supplement request-owned neutral `StreamOptionsWire` fields.
#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OpenAIResponsesOptions {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub reasoning_effort: Option<ThinkingLevel>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub reasoning_summary: Presence<OpenAIResponsesReasoningSummary>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub service_tier: Presence<ResponsesServiceTier>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub tool_choice: Option<OpenAIResponsesToolChoice>,
}

impl fmt::Debug for OpenAIResponsesOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OpenAIResponsesOptions([REDACTED])")
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

/// Complete closed hosted-tool selector from OpenAI 6.40.0 ToolChoiceTypes.
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
    ImageGeneration,
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

/// Passive construction; no provider registration or networking until dispatch.
#[derive(Clone, Debug)]
pub struct OpenAIResponsesApi {
    api: ApiId,
    pub services: AdapterServices,
    pub defaults: OpenAIResponsesOptions,
    /// Bounds aggregate event input, independently of the per-frame transport cap.
    pub max_stream_bytes: usize,
}

impl OpenAIResponsesApi {
    pub fn new(services: AdapterServices) -> Self {
        Self {
            api: ApiId::new("openai-responses").expect("constant API"),
            services,
            defaults: OpenAIResponsesOptions::default(),
            max_stream_bytes: 16 * 1024 * 1024,
        }
    }

    pub fn stream_with_options(
        &self,
        request: TextStreamRequest,
        options: OpenAIResponsesOptions,
    ) -> AssistantStream {
        self.start(request, options, false)
    }

    fn start(
        &self,
        mut request: TextStreamRequest,
        options: OpenAIResponsesOptions,
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
            publish_failure(&request, &producer, output, Failure::Setup("api_mismatch"));
            return stream;
        }
        let services = self.services.clone();
        let max_stream_bytes = self.max_stream_bytes;
        let spawner = request.options.runtime.spawner.clone();
        let worker = producer.clone();
        let task = Box::pin(async move {
            let result = async {
                // Trait contract uses in-band setup failure, including simple auth preflight.
                if simple {
                    client_api_key(&request)?;
                    request.options.wire = build_base_options(
                        &request.resolved.model,
                        &request.context,
                        &request.options.wire,
                    )
                    .map_err(|_| Failure::Setup("context_estimate"))?;
                }
                run(
                    &request,
                    &options,
                    &services,
                    max_stream_bytes,
                    &worker,
                    &mut output,
                )
                .await
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

impl ApiAdapter for OpenAIResponsesApi {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        self.stream_with_options(request, self.defaults.clone())
    }
    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        let reasoning_effort = request.options.wire.reasoning.and_then(|level| {
            let level = clamp_thinking_level(&request.resolved.model, model_level(level));
            match level {
                ModelThinkingLevel::Off => None,
                ModelThinkingLevel::Minimal => Some(ThinkingLevel::Minimal),
                ModelThinkingLevel::Low => Some(ThinkingLevel::Low),
                ModelThinkingLevel::Medium => Some(ThinkingLevel::Medium),
                ModelThinkingLevel::High => Some(ThinkingLevel::High),
                ModelThinkingLevel::ExtraHigh => Some(ThinkingLevel::ExtraHigh),
                ModelThinkingLevel::Max => Some(ThinkingLevel::Max),
            }
        });
        let options = OpenAIResponsesOptions {
            reasoning_effort,
            tool_choice: request.options.wire.tool_choice.map(|choice| {
                OpenAIResponsesToolChoice::Mode(match choice {
                    ToolChoice::Auto => OpenAIResponsesToolChoiceMode::Auto,
                    ToolChoice::None => OpenAIResponsesToolChoiceMode::None,
                })
            }),
            ..Default::default()
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

fn model_level(level: ThinkingLevel) -> ModelThinkingLevel {
    match level {
        ThinkingLevel::Minimal => ModelThinkingLevel::Minimal,
        ThinkingLevel::Low => ModelThinkingLevel::Low,
        ThinkingLevel::Medium => ModelThinkingLevel::Medium,
        ThinkingLevel::High => ModelThinkingLevel::High,
        ThinkingLevel::ExtraHigh => ModelThinkingLevel::ExtraHigh,
        ThinkingLevel::Max => ModelThinkingLevel::Max,
    }
}

fn compat_bool(model: &Model, key: &str, default: bool) -> bool {
    match model.compat.as_ref().and_then(|v| v.0.as_map().get(key)) {
        Some(JsonValue::Bool(v)) => *v,
        _ => default,
    }
}
fn cache_retention(request: &TextStreamRequest, services: &AdapterServices) -> CacheRetention {
    request.options.wire.cache_retention.unwrap_or_else(|| {
        let value = request
            .auth
            .env
            .as_map()
            .get("PI_CACHE_RETENTION")
            .filter(|v| !v.is_empty())
            .or_else(|| services.env.as_map().get("PI_CACHE_RETENTION"));
        if value.is_some_and(|v| v == "long") {
            CacheRetention::Long
        } else {
            CacheRetention::Short
        }
    })
}

fn client_api_key(request: &TextStreamRequest) -> Result<&str, Failure> {
    if let Some(key) = request
        .auth
        .api_key
        .as_ref()
        .map(|key| key.expose_secret())
        .filter(|key| !key.is_empty())
    {
        return Ok(key);
    }
    if request.auth.headers.as_map().iter().any(|(key, value)| {
        (key.eq_ignore_ascii_case("authorization")
            || key.eq_ignore_ascii_case("cf-aig-authorization"))
            && value
                .as_ref()
                .is_some_and(|value| !value.trim_matches(ecma_whitespace).is_empty())
    }) {
        return Ok("unused");
    }
    Err(Failure::Setup("authentication"))
}

fn ecma_whitespace(c: char) -> bool {
    matches!(c, '\u{9}'..='\u{d}' | '\u{20}' | '\u{a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}

fn payload(
    request: &TextStreamRequest,
    options: &OpenAIResponsesOptions,
    cache: CacheRetention,
    grammar: &BTreeMap<String, String>,
) -> Result<JsonValue, Failure> {
    let model = &request.resolved.model;
    let wire = &request.options.wire;
    let mode = if compat_bool(model, "supportsAdditionalTools", false) {
        Some(ResponsesDeferredToolsMode::AdditionalTools)
    } else if compat_bool(model, "supportsToolSearch", false) {
        Some(ResponsesDeferredToolsMode::ToolSearch)
    } else {
        None
    };
    let placement = split_deferred_tools(&request.context, mode.is_some(), None);
    let deferred = placement
        .deferred
        .into_iter()
        .map(|v| (v.normalized_name.as_str().to_owned(), v.tool))
        .collect();
    let tool_options = ConvertResponsesToolsOptions {
        supports_strict_mode: Some(compat_bool(model, "supportsStrictMode", false)),
        supports_openai_grammar_tools: Some(compat_bool(
            model,
            "supportsOpenAIGrammarTools",
            false,
        )),
        ..Default::default()
    };
    let input = convert_responses_messages(
        model,
        &request.context,
        &BTreeSet::from([
            "openai".to_owned(),
            "openai-codex".to_owned(),
            "opencode".to_owned(),
        ]),
        &ConvertResponsesMessagesOptions {
            grammar_tool_input_properties: Some(grammar),
            deferred_tools: Some(&deferred),
            deferred_tools_mode: mode,
            tool_options: Some(&tool_options),
            ..Default::default()
        },
    )?;
    let mut params = json!({"model":model.id,"input":input,"stream":true,"store":false});
    if cache != CacheRetention::None {
        if let Some(key) = clamp_openai_prompt_cache_key(wire.session_id.as_deref()) {
            params["prompt_cache_key"] = json!(key);
        }
    } else if compat_bool(model, "supportsExplicitPromptCacheMode", false) {
        params["prompt_cache_options"] = json!({"mode":"explicit"});
    }
    if cache == CacheRetention::Long && compat_bool(model, "supportsLongCacheRetention", true) {
        params["prompt_cache_retention"] = json!("24h");
    }
    if let Some(max) = wire.max_tokens.filter(|v| v.get() > 0) {
        params["max_output_tokens"] = json!(max.get().max(16));
    }
    if let Some(value) = wire.temperature {
        params["temperature"] = json!(value);
    }
    if !options.service_tier.is_missing() {
        params["service_tier"] = json!(options.service_tier);
    }
    if !placement.immediate.is_empty() {
        params["tools"] = json!(convert_responses_tools(
            &placement.immediate,
            &tool_options
        )?);
    }
    if let Some(value) = &options.tool_choice {
        params["tool_choice"] = json!(value);
    }
    if model.reasoning {
        let mapping = model.thinking_level_map.as_ref().map(|v| v.as_map());
        if options.reasoning_effort.is_some()
            || matches!(options.reasoning_summary, Presence::Value(_))
        {
            let effort = match options.reasoning_effort {
                Some(level) => mapping
                    .and_then(|v| v.get(&model_level(level)))
                    .and_then(Option::as_ref)
                    .map_or_else(|| json!(level), |v| json!(v)),
                None => json!("medium"),
            };
            let summary = match options.reasoning_summary {
                Presence::Value(v) => json!(v),
                _ => json!("auto"),
            };
            params["reasoning"] = json!({"effort":effort,"summary":summary});
            params["include"] = json!(["reasoning.encrypted_content"]);
        } else if model.provider.as_str() != "github-copilot"
            && mapping.and_then(|v| v.get(&ModelThinkingLevel::Off)) != Some(&None)
        {
            params["reasoning"] = json!({"effort":mapping.and_then(|v| v.get(&ModelThinkingLevel::Off)).and_then(Option::as_deref).unwrap_or("none")});
        }
        if model.provider.as_str() == "xai" {
            params["include"] = json!(["reasoning.encrypted_content"]);
        }
    }
    if let Some(sampling) = &wire.sampling_params {
        for (key, value) in sampling.as_map() {
            params[key] = json!(value);
        }
    }
    serde_json::from_value(params).map_err(|_| Failure::Setup("payload"))
}

fn set_header(headers: &mut HttpHeaders, key: &str, value: &str) -> Result<(), Failure> {
    if value.chars().any(|c| u32::from(c) > 255) {
        return Err(Failure::Setup("header"));
    }
    headers.set(key, value.trim_matches([' ', '\t', '\r', '\n']))?;
    Ok(())
}

fn headers(
    request: &TextStreamRequest,
    services: &AdapterServices,
    key: Option<&str>,
    cache: CacheRetention,
    has_body: bool,
) -> Result<HttpHeaders, Failure> {
    let mut headers = HttpHeaders::new();
    if let Some(key) = key {
        for (key, value) in [
            ("accept", "application/json"),
            ("user-agent", "pi-ai-rust"),
            ("x-stainless-retry-count", "0"),
        ] {
            set_header(&mut headers, key, value)?;
        }
        let timeout = request
            .options
            .wire
            .timeout_ms
            .map(SafeUnsigned::get)
            .unwrap_or(0);
        if timeout != 0 {
            set_header(
                &mut headers,
                "x-stainless-timeout",
                &(timeout / 1000).to_string(),
            )?;
        }
        for (env, header) in [
            ("OPENAI_ORG_ID", "openai-organization"),
            ("OPENAI_PROJECT_ID", "openai-project"),
        ] {
            if let Some(value) = services.env.as_map().get(env) {
                set_header(&mut headers, header, value)?;
            }
        }
        set_header(&mut headers, "authorization", &format!("Bearer {key}"))?;
    }
    if let Some(custom) = services.env.as_map().get("OPENAI_CUSTOM_HEADERS") {
        for line in custom.split('\n') {
            if let Some((key, value)) = line.split_once(':') {
                set_header(
                    &mut headers,
                    key.trim_matches(ecma_whitespace),
                    value.trim_matches(ecma_whitespace),
                )?;
            }
        }
    }
    // Pi's user agent overrides SDK/env defaults; its identity is normalized.
    set_header(&mut headers, "user-agent", "pi-ai-rust")?;
    if let Some(model_headers) = &request.resolved.model.headers {
        for (name, value) in model_headers.as_map() {
            set_header(&mut headers, name, value)?;
        }
    }
    if request.resolved.model.provider.as_str() == "github-copilot" {
        for (name, value) in build_copilot_dynamic_headers(
            &request.context.messages,
            has_copilot_vision_input(&request.context.messages),
        ) {
            set_header(&mut headers, &name, &value)?;
        }
    }
    if cache != CacheRetention::None
        && let Some(session) = request
            .options
            .wire
            .session_id
            .as_deref()
            .filter(|v| !v.is_empty())
    {
        let configured = request
            .resolved
            .model
            .compat
            .as_ref()
            .and_then(|v| v.0.as_map().get("sessionAffinityFormat"));
        let format = match configured {
            Some(JsonValue::String(value)) => value.as_str(),
            _ if request.resolved.model.provider.as_str() == "openrouter"
                || request
                    .resolved
                    .endpoint
                    .expose_for_transport()
                    .contains("openrouter.ai") =>
            {
                "openrouter"
            }
            _ => "openai",
        };
        if format == "openrouter" {
            set_header(&mut headers, "x-session-id", session)?;
        } else {
            if format == "openai" {
                set_header(&mut headers, "session_id", session)?;
            }
            set_header(&mut headers, "x-client-request-id", session)?;
        }
    }
    for (name, value) in request.auth.headers.as_map() {
        match value {
            Some(value) => set_header(&mut headers, name, value)?,
            None => {
                let mut checked = HttpHeaders::new();
                checked.set(name, "")?;
                headers.remove(name);
            }
        }
    }
    // SDK body encoder runs after defaultHeaders, so defaults cannot delete this.
    if has_body {
        set_header(&mut headers, "content-type", "application/json")?;
    }
    Ok(headers)
}

fn sdk_attempt<'a>(
    http: &'a HttpRequest,
    request: &'a TextStreamRequest,
    services: &'a AdapterServices,
    output: &'a AssistantMessage,
    streaming: bool,
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
                let kind = error.kind();
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
                classify_sdk_error(Some(response.status), &bytes, output),
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
    let media = content_type.split(';').next().unwrap_or_default().trim();
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

async fn run(
    request: &TextStreamRequest,
    options: &OpenAIResponsesOptions,
    services: &AdapterServices,
    max_stream_bytes: usize,
    producer: &AssistantStreamProducer,
    output: &mut AssistantMessage,
) -> Result<(), Failure> {
    let key = client_api_key(request)?;
    let cache = cache_retention(request, services);
    let grammar = create_grammar_tool_input_properties(
        request.context.tools.as_deref(),
        compat_bool(&request.resolved.model, "supportsOpenAIGrammarTools", false),
    )
    .map_err(|_| Failure::Setup("grammar"))?;
    // With OPENAI_CUSTOM_HEADERS, SDK construction eagerly merges/validates
    // client defaults. Auth and organization/project headers are added later.
    if services
        .env
        .as_map()
        .get("OPENAI_CUSTOM_HEADERS")
        .is_some_and(|value| !value.is_empty())
    {
        headers(request, services, None, cache, false)?;
    }
    let mut payload = payload(request, options, cache, &grammar)?;
    let runtime = &request.options.runtime;
    if let Some(middleware) = &services.payload
        && let Some(replacement) = cancellable(
            middleware.on_payload(request, &mut payload),
            &runtime.cancellation,
        )
        .await
        .map_err(|_| Failure::Transport(TransportErrorKind::Cancelled))?
        .map_err(|_| Failure::Setup("payload_middleware"))?
    {
        payload = replacement;
    }
    let value = serde_json::to_value(&payload).map_err(|_| Failure::Setup("payload"))?;
    if value.is_null() {
        return Err(Failure::Setup("payload"));
    }
    let streaming = value.get("stream").is_some_and(truthy);
    let has_body = truthy(&value);
    let body = if has_body {
        stringify_wire_json(&value)
            .map_err(|_| Failure::Setup("payload"))?
            .into_bytes()
    } else {
        Vec::new()
    };
    let base = request.resolved.endpoint.expose_for_transport();
    let mut url = Url::parse(&format!(
        "{base}{}",
        if base.ends_with('/') {
            "responses"
        } else {
            "/responses"
        }
    ))
    .map_err(|_| Failure::Setup("endpoint"))?;
    // SDK Object.fromEntries/search reserialization: duplicate query keys keep the last value.
    if url.query_pairs().next().is_some() {
        let mut pairs: Vec<(String, String)> = Vec::new();
        for (key, value) in url.query_pairs() {
            if let Some((_, old)) = pairs.iter_mut().find(|(name, _)| name == &key) {
                *old = value.into_owned();
            } else {
                pairs.push((key.into_owned(), value.into_owned()));
            }
        }
        pairs.sort_by_key(|(key, _)| {
            key.parse::<u32>()
                .ok()
                .filter(|v| *v != u32::MAX && v.to_string() == *key)
                .map_or((1, 0), |v| (0, v))
        });
        let query = pairs
            .iter()
            .map(|(key, value)| format!("{}={}", encode_query(key), encode_query(value)))
            .collect::<Vec<_>>()
            .join("&");
        url.set_query(Some(&query));
    }
    let http = HttpRequest {
        endpoint: RequestEndpoint::new_resolved(url.as_str())
            .map_err(|_| Failure::Setup("endpoint"))?,
        method: "POST".to_owned(),
        headers: headers(request, services, Some(key), cache, has_body)?,
        body,
        proxy: None,
    };
    let (response, buffered) = services
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
            || sdk_attempt(&http, request, services, output, streaming),
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
        if matches!(response.status, 204 | 205) {
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
        })
    };
    process_responses_stream(
        source.as_mut(),
        output,
        producer,
        &request.resolved.model,
        &OpenAIResponsesStreamOptions {
            service_tier: options.service_tier.clone(),
            grammar_tool_input_properties: Some(&grammar),
            service_tier_policy: Some(&Pricing(&request.resolved.model)),
        },
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

fn encode_query(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
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
                        return Err(classify_sdk_error(None, &bytes, &self.template).normal);
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

struct Pricing<'a>(&'a Model);
impl ResponsesServiceTierPolicy for Pricing<'_> {
    fn pricing(&self) -> Option<&dyn ResponsesServiceTierPricing> {
        Some(self)
    }
}
impl ResponsesServiceTierPricing for Pricing<'_> {
    fn apply(
        &self,
        usage: &mut Usage,
        tier: Presence<ResponsesServiceTier>,
    ) -> Result<(), ResponsesError> {
        let multiplier = match tier {
            Presence::Value(ResponsesServiceTier::Flex) => 0.5,
            Presence::Value(ResponsesServiceTier::Priority) if self.0.id.as_str() == "gpt-5.5" => {
                2.5
            }
            Presence::Value(ResponsesServiceTier::Priority) => 2.0,
            _ => return Ok(()),
        };
        for value in [
            &mut usage.cost.input,
            &mut usage.cost.output,
            &mut usage.cost.cache_read,
            &mut usage.cost.cache_write,
        ] {
            *value = FiniteF64::new(value.get() * multiplier)
                .map_err(|_| ResponsesError::new(ResponsesErrorKind::Policy))?;
        }
        usage.cost.total = FiniteF64::new(
            usage.cost.input.get()
                + usage.cost.output.get()
                + usage.cost.cache_read.get()
                + usage.cost.cache_write.get(),
        )
        .map_err(|_| ResponsesError::new(ResponsesErrorKind::Policy))?;
        Ok(())
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
fn classify_message(message: &AssistantMessage) -> ResponsesError {
    ResponsesError {
        kind: ResponsesErrorKind::Provider,
        retryable: is_retryable_assistant_error(message),
        context_overflow: is_context_overflow(message, None),
    }
}
struct SdkErrorClassification {
    normal: ResponsesError,
    retry_cap: ResponsesError,
}

fn classify_sdk_error(
    status: Option<u16>,
    bytes: &[u8],
    template: &AssistantMessage,
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
        text = format!("OpenAI API error ({status}): {text}");
    }
    probe.error_message = Some(text);
    SdkErrorClassification {
        normal: classify_message(&probe),
        retry_cap,
    }
}

enum Failure {
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
fn publish_failure(
    request: &TextStreamRequest,
    producer: &AssistantStreamProducer,
    mut output: AssistantMessage,
    error: Failure,
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
                _ => "OpenAI Responses request failed",
            }
        }
        .to_owned(),
    );
    if !aborted {
        let details = serde_json::from_value(json!({"kind":kind,"status":status,"retryable":classified.retryable,"contextOverflow":classified.context_overflow})).expect("safe diagnostic");
        output.diagnostics = Some(vec![AssistantMessageDiagnostic {
            diagnostic_type: "openai_responses_failure"
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

#[cfg(test)]
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
