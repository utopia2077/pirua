//! Host-owned custom providers read from `models.json`.
//!
//! The accepted subset of Pi's `models.json` defines providers on the retained
//! `openai-completions` wire, such as an OpenAI-compatible gateway. Model fields
//! left unset take the upstream defaults, and provider `compat` applies to every
//! model. Fields outside this subset are rejected rather than ignored; the accepted
//! fields are documented in `docs/tui.md`.
use crate::composition::AppError;
use pi_ai::*;
use pi_ai_providers::{
    api::{openai_completions::OpenAICompletionsApi, runtime::AdapterServices},
    auth::{EnvApiKeyAuth, env_api_key_auth},
};
use pi_ai_transport::HttpTransport;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeSet,
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

const API: &str = "openai-completions";
/// The largest integer a JSON number carries exactly, as pirua's counts require.
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// Parsed provider configuration. `Debug` lists provider ids only, never keys.
#[derive(Default)]
pub struct ModelsConfig {
    source: PathBuf,
    providers: Vec<ConfiguredProvider>,
}

impl fmt::Debug for ModelsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.providers.iter().map(|p| p.id.as_str()))
            .finish()
    }
}

struct ConfiguredProvider {
    id: ProviderId,
    name: String,
    api_key: Option<Vec<KeyPart>>,
    models: Vec<Model>,
}

/// One piece of an `apiKey` value, which follows Pi's configuration-value template.
#[derive(Clone)]
enum KeyPart {
    Literal(String),
    Env(String),
}

/// An optional field that, when present, must not be `null`; Pi's schema has no nulls.
fn present<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileEntry {
    #[serde(rename = "$schema", default, deserialize_with = "present")]
    _schema: Option<String>,
    /// Insertion-ordered, so providers register in file order as upstream does.
    providers: Map<String, Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProviderEntry {
    #[serde(default, deserialize_with = "present")]
    name: Option<String>,
    base_url: String,
    api: String,
    /// Read as raw JSON so that a mistyped key is never echoed in an error.
    #[serde(default, deserialize_with = "present")]
    api_key: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    compat: Option<Map<String, Value>>,
    models: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModelEntry {
    id: String,
    #[serde(default, deserialize_with = "present")]
    name: Option<String>,
    #[serde(default, deserialize_with = "present")]
    reasoning: Option<bool>,
    #[serde(default, deserialize_with = "present")]
    thinking_level_map: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    input: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    cost: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    context_window: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    max_tokens: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    compat: Option<Map<String, Value>>,
}

impl ModelsConfig {
    /// Loads `path`; a missing file configures nothing. Errors name the file and
    /// the offending provider or model.
    pub fn load(path: &Path) -> Result<Self, AppError> {
        let located =
            |message: String| -> AppError { format!("{}: {message}", path.display()).into() };
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => return Err(located(error.to_string())),
        };
        let file: FileEntry = serde_json::from_str(text.strip_prefix('\u{feff}').unwrap_or(&text))
            .map_err(|error| located(error.to_string()))?;
        let mut seen: Vec<String> = Vec::new();
        let mut providers = Vec::new();
        for (id, value) in file.providers {
            // The client matches provider prefixes without regard to case.
            if let Some(first) = seen.iter().find(|first| first.eq_ignore_ascii_case(&id)) {
                return Err(located(format!(
                    "provider {id:?} differs from {first:?} only in letter case"
                )));
            }
            providers.push(ConfiguredProvider::parse(&id, value).map_err(located)?);
            seen.push(id);
        }
        Ok(Self {
            source: path.to_owned(),
            providers,
        })
    }
}

impl ConfiguredProvider {
    fn parse(id: &str, value: Value) -> Result<Self, String> {
        let at = format!("provider {id:?}");
        if id.is_empty()
            || !id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        {
            return Err(format!(
                "{at}: provider ids may only contain ASCII letters, digits, '.', '_' and '-'"
            ));
        }
        // Built-ins are reserved before an entry is read; a test keeps this list in step
        // with the live registry, which registration re-checks. The client matches
        // provider prefixes without regard to case.
        if ProviderId::KNOWN
            .iter()
            .any(|built_in| built_in.eq_ignore_ascii_case(id))
        {
            return Err(format!("{at} is built in and cannot be redefined"));
        }
        let entry: ProviderEntry =
            serde_json::from_value(value).map_err(|error| format!("{at}: {error}"))?;
        if entry.name.as_deref() == Some("") {
            return Err(format!("{at}: name must not be empty"));
        }
        if entry.api != API {
            return Err(format!("{at}: api must be \"{API}\""));
        }
        let Some(url) = url::Url::parse(&entry.base_url)
            .ok()
            .filter(|url| matches!(url.scheme(), "http" | "https") && url.has_host())
        else {
            return Err(format!(
                "{at}: baseUrl must be an absolute http or https URL"
            ));
        };
        // The adapter appends `/chat/completions` to the configured text as written.
        if entry.base_url.trim() != entry.base_url {
            return Err(format!(
                "{at}: baseUrl must not have surrounding whitespace"
            ));
        }
        // Model endpoints are public metadata; credentials belong in apiKey.
        if !url.username().is_empty() || url.password().is_some() {
            return Err(format!(
                "{at}: baseUrl must not embed credentials; use apiKey"
            ));
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(format!(
                "{at}: baseUrl must not contain a query or fragment"
            ));
        }
        let api_key = match entry.api_key {
            None => None,
            Some(Value::String(key)) if key.is_empty() => {
                return Err(format!("{at}: apiKey must not be empty"));
            }
            Some(Value::String(key)) if key.starts_with('!') => {
                return Err(format!(
                    "{at}: apiKey commands (\"!...\") are not supported"
                ));
            }
            Some(Value::String(key)) => Some(parse_template(&key)),
            Some(_) => return Err(format!("{at}: apiKey must be a string")),
        };
        if entry.models.is_empty() {
            return Err(format!("{at}: at least one model is required"));
        }
        let mut seen = BTreeSet::new();
        let mut models = Vec::new();
        for (index, value) in entry.models.into_iter().enumerate() {
            let model_at = match value.get("id").and_then(Value::as_str) {
                Some(model) => format!("{at} model {model:?}"),
                None => format!("{at} model #{index}"),
            };
            let mut model: ModelEntry =
                serde_json::from_value(value).map_err(|error| format!("{model_at}: {error}"))?;
            if !seen.insert(model.id.clone()) {
                return Err(format!("{model_at}: duplicate model id"));
            }
            if model.name.as_deref() == Some("") {
                return Err(format!("{model_at}: name must not be empty"));
            }
            // JSON numbers such as 1e6 are integral counts, as upstream accepts them.
            for (field, count) in [
                ("contextWindow", &mut model.context_window),
                ("maxTokens", &mut model.max_tokens),
            ] {
                if let Some(count) = count {
                    match count.as_f64() {
                        Some(n) if n >= 1.0 && n.fract() == 0.0 && n <= MAX_SAFE_INTEGER => {
                            *count = json!(n as u64);
                        }
                        _ => return Err(format!("{model_at}: {field} must be a positive integer")),
                    }
                }
            }
            let mut value = json!({
                "id": model.id,
                "name": model.name.unwrap_or_else(|| model.id.clone()),
                "api": API,
                "provider": id,
                "baseUrl": entry.base_url,
                "reasoning": model.reasoning.unwrap_or(false),
                "input": model.input.unwrap_or_else(|| json!(["text"])),
                "cost": model.cost.unwrap_or_else(|| json!({"input":0,"output":0,"cacheRead":0,"cacheWrite":0})),
                "contextWindow": model.context_window.unwrap_or_else(|| json!(128000)),
                "maxTokens": model.max_tokens.unwrap_or_else(|| json!(16384)),
            });
            if let Some(map) = model.thinking_level_map {
                value["thinkingLevelMap"] = map;
            }
            let compat = merge_compat(entry.compat.as_ref(), model.compat.unwrap_or_default());
            if !compat.is_empty() {
                value["compat"] = Value::Object(compat);
            }
            models.push(
                serde_json::from_value::<Model>(value)
                    .map_err(|error| format!("{model_at}: {error}"))?,
            );
        }
        Ok(Self {
            id: ProviderId::new(id).map_err(|error| format!("{at}: {error}"))?,
            name: entry.name.unwrap_or_else(|| id.to_owned()),
            api_key,
            models,
        })
    }
}

/// Model `compat` overrides provider `compat`; these nested objects merge per key,
/// as in Pi's provider composition.
fn merge_compat(
    provider: Option<&Map<String, Value>>,
    model: Map<String, Value>,
) -> Map<String, Value> {
    const NESTED: [&str; 4] = [
        "openRouterRouting",
        "vercelGatewayRouting",
        "chatTemplateKwargs",
        "chatTemplateArgs",
    ];
    let mut merged = provider.cloned().unwrap_or_default();
    for (key, value) in model {
        if NESTED.contains(&key.as_str())
            && let (Some(Value::Object(base)), Value::Object(nested)) =
                (merged.get_mut(&key), &value)
        {
            base.extend(nested.clone());
            continue;
        }
        merged.insert(key, value);
    }
    merged
}

/// Registers every configured provider in `registry`. Nothing is registered when
/// any configured provider would replace a registered one.
pub async fn register_configured_providers(
    registry: &ModelsRegistry,
    config: &ModelsConfig,
    transport: Arc<HttpTransport>,
) -> Result<(), AppError> {
    if let Some(provider) = config
        .providers
        .iter()
        .find(|p| registry.get_provider(&p.id).is_some())
    {
        return Err(format!(
            "{}: provider {:?} is already registered",
            config.source.display(),
            provider.id.as_str()
        )
        .into());
    }
    for provider in &config.providers {
        let adapter: Arc<dyn ApiAdapter> = Arc::new(OpenAICompletionsApi::new(
            AdapterServices::new(transport.clone()),
        ));
        let auth = ProviderAuth::new(
            Some(Arc::new(ConfiguredApiKeyAuth {
                stored: env_api_key_auth(format!("{} API key", provider.name), Vec::new()),
                template: provider.api_key.clone(),
            })),
            None,
        )?;
        registry
            .set_provider(create_provider(CreateProviderOptions {
                id: provider.id.clone(),
                name: Some(provider.name.clone()),
                auth,
                models: provider.models.clone(),
                dynamic_models: None,
                model_filter: None,
                adapters: vec![adapter],
            })?)
            .await?;
    }
    Ok(())
}

/// `$NAME` and `${NAME}` read the environment; `$$` and `$!` escape `$` and `!`.
/// Any other `$` is literal, as is `${...}` around an invalid name.
fn parse_template(config: &str) -> Vec<KeyPart> {
    fn name_len(text: &str) -> usize {
        let mut chars = text.char_indices();
        match chars.next() {
            Some((_, c)) if c.is_ascii_alphabetic() || c == '_' => chars
                .find(|(_, c)| !(c.is_ascii_alphanumeric() || *c == '_'))
                .map_or(text.len(), |(i, _)| i),
            _ => 0,
        }
    }
    let mut parts = Vec::new();
    let mut literal = String::new();
    let mut env = |literal: &mut String, name: &str| {
        if !literal.is_empty() {
            parts.push(KeyPart::Literal(std::mem::take(literal)));
        }
        parts.push(KeyPart::Env(name.to_owned()));
    };
    let mut rest = config;
    while let Some(dollar) = rest.find('$') {
        literal.push_str(&rest[..dollar]);
        let after = &rest[dollar + 1..];
        rest = if let Some(escaped) = after.strip_prefix(['$', '!']) {
            literal.push_str(&after[..1]);
            escaped
        } else if let Some(braced) = after.strip_prefix('{') {
            match braced.find('}') {
                Some(end) if name_len(&braced[..end]) == end && end > 0 => {
                    env(&mut literal, &braced[..end]);
                    &braced[end + 1..]
                }
                Some(end) => {
                    literal.push_str(&rest[dollar..dollar + end + 3]);
                    &braced[end + 1..]
                }
                None => {
                    literal.push('$');
                    after
                }
            }
        } else {
            match name_len(after) {
                0 => {
                    literal.push('$');
                    after
                }
                len => {
                    env(&mut literal, &after[..len]);
                    &after[len..]
                }
            }
        };
    }
    literal.push_str(rest);
    if !literal.is_empty() {
        parts.push(KeyPart::Literal(literal));
    }
    parts
}

/// Login and stored keys behave exactly as pirua's built-in API-key auth; the
/// configured value is the fallback when no key has been stored.
struct ConfiguredApiKeyAuth {
    stored: EnvApiKeyAuth,
    template: Option<Vec<KeyPart>>,
}

impl ApiKeyAuth for ConfiguredApiKeyAuth {
    fn name(&self) -> &str {
        self.stored.name()
    }
    fn login<'a>(
        &'a self,
        interaction: &'a dyn AuthInteraction,
        cancellation: &'a CancellationToken,
    ) -> Option<BoxFuture<'a, Result<ApiKeyCredential, AuthProviderError>>> {
        self.stored.login(interaction, cancellation)
    }
    fn resolve<'a>(
        &'a self,
        input: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async move {
            let stored = self
                .stored
                .resolve(ApiKeyAuthInput {
                    context: input.context,
                    credential: input.credential,
                    cancellation: input.cancellation,
                })
                .await?;
            if stored.is_some() {
                return Ok(stored);
            }
            let Some(template) = &self.template else {
                return Ok(None);
            };
            let mut key = String::new();
            for part in template {
                match part {
                    KeyPart::Literal(text) => key.push_str(text),
                    KeyPart::Env(name) => {
                        // An unset or empty variable leaves the key unconfigured.
                        match input
                            .context
                            .env(name, input.cancellation)
                            .await
                            .map_err(AuthProviderError::with_source)?
                        {
                            Some(value) if !value.is_empty() => key.push_str(&value),
                            _ => return Ok(None),
                        }
                    }
                }
            }
            Ok(Some(AuthResult {
                auth: ModelAuth {
                    api_key: Some(SecretString::new(key)),
                    ..Default::default()
                },
                env: None,
                source: Some("configured API key".into()),
            }))
        })
    }
}
