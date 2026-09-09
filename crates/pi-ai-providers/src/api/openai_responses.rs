//! Standard OpenAI Responses wrapper, pinned to pi-mono 853a80d and OpenAI 6.40.0.
//! Neutral options, auth and endpoint custody remain on the nominal request.
//! SDK platform identity is normalized to Rust; no environment is read implicitly.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use pi_ai_core::{
    ApiAdapter, ApiId, AssistantMessage, AssistantStream, AssistantStreamInit,
    AssistantStreamProducer, BoxFuture, CacheRetention, CapabilityError, DeferredCancelRequest,
    DeferredFetchRequest, FiniteF64, JsonValue, Model, ModelThinkingLevel, ModelsError,
    ModelsErrorCode, Presence, RequestEndpoint, SafeUnsigned, SimpleStreamRequest, StreamOptions,
    StreamSetupFailure, TextStreamRequest, ThinkingLevel, ToolChoice, Usage, clamp_thinking_level,
    create_assistant_message_event_stream, split_deferred_tools,
};
use pi_ai_transport::{HttpHeaders, HttpRequest, TransportErrorKind};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;

use super::constrained_sampling::create_grammar_tool_input_properties;
use super::github_copilot_headers::{build_copilot_dynamic_headers, has_copilot_vision_input};
use super::openai_prompt_cache::clamp_openai_prompt_cache_key;
use super::openai_responses_shared::{
    ConvertResponsesMessagesOptions, ConvertResponsesToolsOptions, OpenAIResponsesStreamOptions,
    ResponsesDeferredToolsMode, ResponsesError, ResponsesErrorKind, ResponsesServiceTier,
    ResponsesServiceTierPolicy, ResponsesServiceTierPricing, convert_responses_messages,
    convert_responses_tools,
};
use super::openai_sdk_strings;
use super::runtime::{AdapterServices, cancellable};
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

use super::openai_sdk::{Failure, Family, empty_message};
pub use super::openai_sdk::{
    OpenAIResponsesAllowedToolsMode, OpenAIResponsesHostedTool, OpenAIResponsesReasoningSummary,
    OpenAIResponsesToolChoice, OpenAIResponsesToolChoiceMode, OpenAIResponsesToolSelector,
};
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
            super::openai_sdk::publish_failure(
                &request,
                &producer,
                output,
                Failure::Setup("api_mismatch"),
                Family::Standard,
            );
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
                super::openai_sdk::publish_failure(
                    &request,
                    &worker,
                    output,
                    error,
                    Family::Standard,
                );
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
) -> Result<super::wire_json::WirePayload, Failure> {
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
    Ok(super::wire_json::WirePayload::new(params))
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
            if let Some(value) = openai_sdk_strings::read_env(&services.env, env) {
                set_header(&mut headers, header, value)?;
            }
        }
        set_header(&mut headers, "authorization", &format!("Bearer {key}"))?;
    }
    if let Some(custom) = openai_sdk_strings::read_env(&services.env, "OPENAI_CUSTOM_HEADERS") {
        for (key, value) in openai_sdk_strings::custom_headers(custom) {
            set_header(&mut headers, key, value)?;
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
    for (name, value) in request.auth.headers.iter_for_dispatch() {
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
    if openai_sdk_strings::read_env(&services.env, "OPENAI_CUSTOM_HEADERS").is_some() {
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
    let value = payload.into_value();
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
    super::openai_sdk::execute(
        &http,
        request,
        services,
        super::openai_sdk::Execution {
            streaming,
            max_stream_bytes,
            stream_options: OpenAIResponsesStreamOptions {
                preserve_end_turn: false,
                service_tier: options.service_tier.clone(),
                grammar_tool_input_properties: Some(&grammar),
                service_tier_policy: Some(&Pricing(&request.resolved.model)),
            },
            family: Family::Standard,
        },
        producer,
        output,
    )
    .await
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
