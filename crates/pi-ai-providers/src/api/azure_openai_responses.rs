//! Azure Responses: source-specific configuration and payload over private SDK execution.
//! Raw option Serde is transient input, never a durable or foreign authorization grant.
pub use super::openai_sdk::{
    OpenAIResponsesAllowedToolsMode, OpenAIResponsesHostedTool,
    OpenAIResponsesReasoningSummary as AzureOpenAIResponsesReasoningSummary,
    OpenAIResponsesToolChoice as AzureOpenAIResponsesToolChoice, OpenAIResponsesToolChoiceMode,
    OpenAIResponsesToolSelector,
};
use super::{
    constrained_sampling::create_grammar_tool_input_properties,
    openai_prompt_cache::clamp_openai_prompt_cache_key,
    openai_responses_shared::{
        ConvertResponsesMessagesOptions, ConvertResponsesToolsOptions,
        OpenAIResponsesStreamOptions, convert_responses_messages, convert_responses_tools,
    },
    openai_sdk::{self, Failure, Family, empty_message},
    runtime::{AdapterServices, cancellable},
    simple_options::build_base_options,
    wire_json::stringify_wire_json,
};
use pi_ai_core::*;
use pi_ai_transport::{HttpHeaders, HttpRequest, TransportErrorKind};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};
use url::Url;
fn optional_non_null<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(d)?
        .map(Some)
        .ok_or_else(|| serde::de::Error::custom("expected non-null optional value"))
}
#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AzureOpenAIResponsesOptions {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub reasoning_effort: Option<ThinkingLevel>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub tool_choice: Option<AzureOpenAIResponsesToolChoice>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub azure_api_version: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub azure_resource_name: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub azure_base_url: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub azure_deployment_name: Option<String>,
    #[serde(default, skip_serializing_if = "Presence::is_missing")]
    pub reasoning_summary: Presence<AzureOpenAIResponsesReasoningSummary>,
}
impl fmt::Debug for AzureOpenAIResponsesOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AzureOpenAIResponsesOptions([REDACTED])")
    }
}
#[derive(Clone, Debug)]
pub struct AzureOpenAIResponsesApi {
    api: ApiId,
    pub services: AdapterServices,
    pub defaults: AzureOpenAIResponsesOptions,
    pub max_stream_bytes: usize,
}
/// Passive native counterpart of the source lazy factory. No global registration,
/// client construction, credential discovery or network action occurs here.
pub fn azure_openai_responses_api(services: AdapterServices) -> AzureOpenAIResponsesApi {
    AzureOpenAIResponsesApi::new(services)
}
impl AzureOpenAIResponsesApi {
    pub fn new(services: AdapterServices) -> Self {
        Self {
            api: "azure-openai-responses".parse().expect("constant API"),
            services,
            defaults: Default::default(),
            max_stream_bytes: 16 * 1024 * 1024,
        }
    }
    pub fn stream_with_options(
        &self,
        request: TextStreamRequest,
        options: AzureOpenAIResponsesOptions,
    ) -> AssistantStream {
        self.start(request, options, false)
    }
    fn start(
        &self,
        mut request: TextStreamRequest,
        options: AzureOpenAIResponsesOptions,
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
            openai_sdk::publish_failure(
                &request,
                &producer,
                output,
                Failure::Setup("api_mismatch"),
                Family::Azure,
            );
            return stream;
        }
        let services = self.services.clone();
        let limit = self.max_stream_bytes;
        let spawner = request.options.runtime.spawner.clone();
        let worker = producer.clone();
        if spawner
            .spawn(Box::pin(async move {
                let result = async {
                    if simple {
                        api_key(&request)?;
                        request.options.wire = build_base_options(
                            &request.resolved.model,
                            &request.context,
                            &request.options.wire,
                        )
                        .map_err(|_| Failure::Setup("context_estimate"))?;
                    }
                    run(&request, &options, &services, limit, &worker, &mut output).await
                }
                .await;
                if let Err(error) = result {
                    openai_sdk::publish_failure(&request, &worker, output, error, Family::Azure);
                }
            }))
            .is_err()
        {
            let _ = producer.fail_setup(StreamSetupFailure::Spawn);
        }
        stream
    }
}
impl ApiAdapter for AzureOpenAIResponsesApi {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, request: TextStreamRequest) -> AssistantStream {
        self.stream_with_options(request, self.defaults.clone())
    }
    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        let reasoning_effort = request.options.wire.reasoning.and_then(|level| {
            match clamp_thinking_level(&request.resolved.model, model_level(level)) {
                ModelThinkingLevel::Off => None,
                ModelThinkingLevel::Minimal => Some(ThinkingLevel::Minimal),
                ModelThinkingLevel::Low => Some(ThinkingLevel::Low),
                ModelThinkingLevel::Medium => Some(ThinkingLevel::Medium),
                ModelThinkingLevel::High => Some(ThinkingLevel::High),
                ModelThinkingLevel::ExtraHigh => Some(ThinkingLevel::ExtraHigh),
                ModelThinkingLevel::Max => Some(ThinkingLevel::Max),
            }
        });
        let options = AzureOpenAIResponsesOptions {
            reasoning_effort,
            tool_choice: request.options.wire.tool_choice.map(|choice| {
                AzureOpenAIResponsesToolChoice::Mode(match choice {
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
fn whitespace(c: char) -> bool {
    matches!(c,'\u{9}'..='\u{d}'|' '|'\u{a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}'|'\u{feff}')
}
fn env<'a>(name: &str, scoped: &'a ProviderEnv, ambient: &'a ProviderEnv) -> Option<&'a str> {
    scoped
        .as_map()
        .get(name)
        .filter(|s| !s.is_empty())
        .or_else(|| ambient.as_map().get(name).filter(|s| !s.is_empty()))
        .map(String::as_str)
}
fn api_key(request: &TextStreamRequest) -> Result<&str, Failure> {
    request
        .auth
        .api_key
        .as_ref()
        .map(SecretString::expose_secret)
        .filter(|s| !s.is_empty())
        .ok_or(Failure::Setup("authentication"))
}
fn deployment(
    model: &Model,
    options: &AzureOpenAIResponsesOptions,
    scoped: &ProviderEnv,
    ambient: &ProviderEnv,
) -> String {
    if let Some(value) = options
        .azure_deployment_name
        .as_ref()
        .filter(|s| !s.is_empty())
    {
        return value.clone();
    }
    let mut mapping = BTreeMap::new();
    if let Some(value) = env("AZURE_OPENAI_DEPLOYMENT_NAME_MAP", scoped, ambient) {
        for entry in value
            .split(',')
            .map(|s| s.trim_matches(whitespace))
            .filter(|s| !s.is_empty())
        {
            let mut fields = entry.split('=');
            let id = fields.next().unwrap_or_default();
            let name = fields.next().unwrap_or_default();
            if !id.is_empty() && !name.is_empty() {
                mapping.insert(id.trim_matches(whitespace), name.trim_matches(whitespace));
            }
        }
    }
    mapping
        .get(model.id.as_str())
        .filter(|s| !s.is_empty())
        .copied()
        .unwrap_or(model.id.as_str())
        .to_owned()
}
struct AzureConfig {
    base: RequestEndpoint,
    version: String,
}
impl fmt::Debug for AzureConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AzureConfig([REDACTED])")
    }
}
fn resolve_config(
    fallback: Option<&RequestEndpoint>,
    options: &AzureOpenAIResponsesOptions,
    scoped: &ProviderEnv,
    ambient: &ProviderEnv,
) -> Result<AzureConfig, Failure> {
    let version = options
        .azure_api_version
        .as_deref()
        .filter(|s| !s.is_empty())
        .or_else(|| env("AZURE_OPENAI_API_VERSION", scoped, ambient))
        .unwrap_or("v1")
        .to_owned();
    let base = options
        .azure_base_url
        .as_deref()
        .map(|s| s.trim_matches(whitespace))
        .filter(|s| !s.is_empty())
        .or_else(|| {
            env("AZURE_OPENAI_BASE_URL", scoped, ambient)
                .map(|s| s.trim_matches(whitespace))
                .filter(|s| !s.is_empty())
        });
    let resource = options
        .azure_resource_name
        .as_deref()
        .filter(|s| !s.is_empty())
        .or_else(|| env("AZURE_OPENAI_RESOURCE_NAME", scoped, ambient));
    let resource_base = resource.map(|name| format!("https://{name}.openai.azure.com/openai/v1"));
    let raw = base
        .or(resource_base.as_deref())
        .or_else(|| fallback.map(RequestEndpoint::expose_for_transport))
        .ok_or(Failure::Setup("azure_configuration"))?;
    let mut url = Url::parse(raw.trim_matches(whitespace).trim_end_matches('/'))
        .map_err(|_| Failure::Setup("azure_base_url"))?;
    let azure = url.host_str().is_some_and(|host| {
        [
            ".openai.azure.com",
            ".cognitiveservices.azure.com",
            ".ai.azure.com",
        ]
        .iter()
        .any(|suffix| host.ends_with(suffix))
    });
    if azure
        && matches!(
            url.path().trim_end_matches('/'),
            "" | "/" | "/openai" | "/openai/v1/responses"
        )
    {
        url.set_path("/openai/v1");
        url.set_query(None);
    }
    let base = RequestEndpoint::new_resolved(url.as_str().trim_end_matches('/'))
        .map_err(|_| Failure::Setup("azure_base_url"))?;
    Ok(AzureConfig { base, version })
}
fn endpoint(config: &AzureConfig) -> Result<RequestEndpoint, Failure> {
    // Responses is not an Azure SDK deployment endpoint. Model is payload only.
    let mut url = Url::parse(&format!("{}/responses", config.base.expose_for_transport()))
        .map_err(|_| Failure::Setup("endpoint"))?;
    let mut pairs: Vec<(String, String)> = vec![];
    for (name, value) in url.query_pairs() {
        if let Some((_, old)) = pairs.iter_mut().find(|(key, _)| key == &name) {
            *old = value.into_owned();
        } else {
            pairs.push((name.into_owned(), value.into_owned()));
        }
    }
    if let Some((_, value)) = pairs.iter_mut().find(|(key, _)| key == "api-version") {
        *value = config.version.clone();
    } else {
        pairs.push(("api-version".into(), config.version.clone()));
    }
    pairs.sort_by_key(|(key, _)| {
        key.parse::<u32>()
            .ok()
            .filter(|v| *v != u32::MAX && v.to_string() == *key)
            .map_or((1, 0), |v| (0, v))
    });
    let encode = |text: &str| {
        text.bytes()
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
            .map(|(key, value)| format!("{}={}", encode(key), encode(value)))
            .collect::<Vec<_>>()
            .join("&"),
    ));
    RequestEndpoint::new_resolved(url.as_str()).map_err(|_| Failure::Setup("endpoint"))
}
fn set(headers: &mut HttpHeaders, key: &str, value: &str) -> Result<(), Failure> {
    if value.chars().any(|c| u32::from(c) > 255) {
        return Err(Failure::Setup("header"));
    }
    headers.set(key, value.trim_matches([' ', '\t', '\r', '\n']))?;
    Ok(())
}
fn client_defaults(
    request: &TextStreamRequest,
    services: &AdapterServices,
) -> Result<(HttpHeaders, BTreeSet<String>), Failure> {
    fn assign(target: &mut Vec<(String, Option<String>)>, key: &str, value: Option<String>) {
        if let Some((_, old)) = target.iter_mut().find(|(name, _)| name == key) {
            *old = value;
        } else {
            target.push((key.to_owned(), value));
        }
    }
    let mut headers = HttpHeaders::new();
    let mut nulls = BTreeSet::new();
    let mut ambient = Vec::new();
    if let Some(custom) =
        super::openai_sdk_strings::read_env(&services.env, "OPENAI_CUSTOM_HEADERS")
    {
        for (key, value) in super::openai_sdk_strings::custom_headers(custom) {
            ambient.push((key.to_owned(), Some(value.to_owned())));
        }
    }
    // Object.assign overwrites a value without moving its original own-key slot.
    let mut defaults = vec![("User-Agent".into(), Some("pi-ai-rust".into()))];
    if let Some(model) = &request.resolved.model.headers {
        for (key, value) in model.as_map() {
            assign(&mut defaults, key, Some(value.clone()));
        }
    }
    for (key, value) in request.auth.headers.iter_for_dispatch() {
        assign(&mut defaults, key, value.clone());
    }
    for (key, value) in ambient.into_iter().chain(defaults) {
        match value {
            Some(value) => {
                set(&mut headers, &key, &value)?;
                nulls.remove(&key.to_ascii_lowercase());
            }
            None => {
                let mut probe = HttpHeaders::new();
                probe.set(&key, "")?;
                headers.remove(&key);
                nulls.insert(key.to_ascii_lowercase());
            }
        }
    }
    Ok((headers, nulls))
}
fn headers(
    request: &TextStreamRequest,
    services: &AdapterServices,
    key: &str,
    has_body: bool,
) -> Result<HttpHeaders, Failure> {
    let mut headers = HttpHeaders::new();
    for (key, value) in [
        ("accept", "application/json"),
        ("user-agent", "pi-ai-rust"),
        ("x-stainless-retry-count", "0"),
    ] {
        set(&mut headers, key, value)?;
    }
    if let Some(timeout) = request.options.wire.timeout_ms.filter(|v| v.get() != 0) {
        set(
            &mut headers,
            "x-stainless-timeout",
            &(timeout.get() / 1000).to_string(),
        )?;
    }
    for (env, key) in [
        ("OPENAI_ORG_ID", "openai-organization"),
        ("OPENAI_PROJECT_ID", "openai-project"),
    ] {
        if let Some(value) = super::openai_sdk_strings::read_env(&services.env, env) {
            set(&mut headers, key, value)?;
        }
    }
    set(&mut headers, "api-key", key)?;
    let (defaults, nulls) = client_defaults(request, services)?;
    for (key, value) in defaults.iter_for_wire() {
        set(&mut headers, key, value)?;
    }
    // SDK nullable default headers suppress earlier protocol/auth defaults too.
    for key in nulls {
        headers.remove(&key);
    }
    if has_body {
        set(&mut headers, "content-type", "application/json")?;
    }
    Ok(headers)
}
fn compat(model: &Model, key: &str, default: bool) -> bool {
    model
        .compat
        .as_ref()
        .and_then(|v| v.0.as_map().get(key))
        .and_then(|v| match v {
            JsonValue::Bool(v) => Some(*v),
            _ => None,
        })
        .unwrap_or(default)
}
fn build_payload(
    request: &TextStreamRequest,
    options: &AzureOpenAIResponsesOptions,
    deployment: &str,
    grammar: &BTreeMap<String, String>,
) -> Result<JsonValue, Failure> {
    let model = &request.resolved.model;
    let wire = &request.options.wire;
    let input = convert_responses_messages(
        model,
        &request.context,
        &BTreeSet::from([
            "openai".into(),
            "openai-codex".into(),
            "opencode".into(),
            "azure-openai-responses".into(),
        ]),
        &ConvertResponsesMessagesOptions {
            grammar_tool_input_properties: Some(grammar),
            ..Default::default()
        },
    )?;
    let mut value = json!({"model":deployment,"input":input,"stream":true,"store":false});
    if let Some(key) = clamp_openai_prompt_cache_key(wire.session_id.as_deref()) {
        value["prompt_cache_key"] = json!(key);
    }
    if let Some(tokens) = wire.max_tokens.filter(|v| v.get() != 0) {
        value["max_output_tokens"] = json!(tokens.get().max(16));
    }
    if let Some(temperature) = wire.temperature {
        value["temperature"] = json!(temperature);
    }
    if let Some(tools) = request.context.tools.as_ref().filter(|v| !v.is_empty()) {
        value["tools"] = json!(convert_responses_tools(
            tools,
            &ConvertResponsesToolsOptions {
                supports_strict_mode: Some(compat(model, "supportsStrictMode", true)),
                supports_openai_grammar_tools: Some(compat(
                    model,
                    "supportsOpenAIGrammarTools",
                    false
                )),
                ..Default::default()
            }
        )?);
    }
    if let Some(choice) = &options.tool_choice {
        value["tool_choice"] = json!(choice);
    }
    if model.reasoning {
        let map = model.thinking_level_map.as_ref().map(|v| v.as_map());
        if options.reasoning_effort.is_some()
            || matches!(options.reasoning_summary, Presence::Value(_))
        {
            let effort = options
                .reasoning_effort
                .map(|level| {
                    map.and_then(|map| map.get(&model_level(level)))
                        .and_then(Option::as_deref)
                        .map_or_else(|| json!(level), |v| json!(v))
                })
                .unwrap_or(json!("medium"));
            let summary = match options.reasoning_summary {
                Presence::Value(v) => json!(v),
                _ => json!("auto"),
            };
            value["reasoning"] = json!({"effort":effort,"summary":summary});
            value["include"] = json!(["reasoning.encrypted_content"]);
        } else if map.and_then(|map| map.get(&ModelThinkingLevel::Off)) != Some(&None) {
            value["reasoning"] = json!({"effort":map.and_then(|map|map.get(&ModelThinkingLevel::Off)).and_then(Option::as_deref).unwrap_or("none")});
        }
    }
    if let Some(sampling) = &wire.sampling_params {
        for (key, v) in sampling.as_map() {
            value[key] = json!(v);
        }
    }
    serde_json::from_value(value).map_err(|_| Failure::Setup("payload"))
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
async fn run(
    request: &TextStreamRequest,
    options: &AzureOpenAIResponsesOptions,
    services: &AdapterServices,
    limit: usize,
    producer: &AssistantStreamProducer,
    output: &mut AssistantMessage,
) -> Result<(), Failure> {
    let key = api_key(request)?;
    let config = resolve_config(
        Some(&request.resolved.endpoint),
        options,
        &request.auth.env,
        &services.env,
    )?;
    // OPENAI_CUSTOM_HEADERS makes SDK construction eagerly validate defaults.
    if super::openai_sdk_strings::read_env(&services.env, "OPENAI_CUSTOM_HEADERS").is_some() {
        client_defaults(request, services)?;
    }
    let grammar = create_grammar_tool_input_properties(
        request.context.tools.as_deref(),
        compat(&request.resolved.model, "supportsOpenAIGrammarTools", false),
    )
    .map_err(|_| Failure::Setup("grammar"))?;
    let deployment = deployment(
        &request.resolved.model,
        options,
        &request.auth.env,
        &services.env,
    );
    let mut value = build_payload(request, options, &deployment, &grammar)?;
    if let Some(hook) = &services.payload
        && let Some(next) = cancellable(
            hook.on_payload(request, &mut value),
            &request.options.runtime.cancellation,
        )
        .await
        .map_err(|_| Failure::Transport(TransportErrorKind::Cancelled))?
        .map_err(|_| Failure::Setup("payload_middleware"))?
    {
        value = next;
    }
    let value = serde_json::to_value(value).map_err(|_| Failure::Setup("payload"))?;
    if value.is_null() {
        return Err(Failure::Setup("payload"));
    }
    let http = HttpRequest {
        endpoint: endpoint(&config)?,
        method: "POST".into(),
        headers: headers(request, services, key, truthy(&value))?,
        body: if truthy(&value) {
            stringify_wire_json(&value)
                .map_err(|_| Failure::Setup("payload"))?
                .into_bytes()
        } else {
            vec![]
        },
        proxy: None,
    };
    openai_sdk::execute(
        &http,
        request,
        services,
        openai_sdk::Execution {
            streaming: value.get("stream").is_some_and(truthy),
            max_stream_bytes: limit,
            stream_options: OpenAIResponsesStreamOptions {
                grammar_tool_input_properties: Some(&grammar),
                ..Default::default()
            },
            family: Family::Azure,
        },
        producer,
        output,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_private_configuration_fails_without_an_invented_endpoint() {
        let result = resolve_config(
            None,
            &AzureOpenAIResponsesOptions::default(),
            &ProviderEnv::default(),
            &ProviderEnv::default(),
        );
        assert!(matches!(result, Err(Failure::Setup("azure_configuration"))));
    }
}
