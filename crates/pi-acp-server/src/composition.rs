//! Production native providers/auth/catalog, resources, settings and local tools.
use crate::{credentials::Credentials, fixture};
use pi_ai::*;
use pi_ai_providers::{
    api::runtime::AdapterServices,
    auth::{NativeProviderAuthContext, OAuthServices},
};
use pi_ai_transport::*;
use pi_coding_runtime::*;
use pi_coding_session::{SessionManager, SessionStore, SqliteStore};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};
pub type AppError = Box<dyn std::error::Error + Send + Sync>;
#[derive(Clone, Debug)]
pub struct Options {
    pub data_dir: PathBuf,
    pub config_dir: PathBuf,
    pub credentials: Option<PathBuf>,
    pub model: Option<String>,
    pub fixture_provider: bool,
    pub fixture_delay_ms: u64,
}
impl Options {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, AppError> {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        let mut options = Self {
            data_dir: std::env::var_os("XDG_DATA_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".local/share"))
                .join("pirua"),
            config_dir: std::env::var_os("XDG_CONFIG_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".config"))
                .join("pirua"),
            credentials: None,
            model: None,
            fixture_provider: false,
            fixture_delay_ms: 25,
        };
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--fixture-provider" => options.fixture_provider = true,
                "--data-dir" => {
                    options.data_dir =
                        PathBuf::from(args.next().ok_or("--data-dir requires a path")?)
                }
                "--config-dir" => {
                    options.config_dir =
                        PathBuf::from(args.next().ok_or("--config-dir requires a path")?)
                }
                "--credentials" => {
                    options.credentials = Some(PathBuf::from(
                        args.next().ok_or("--credentials requires a path")?,
                    ))
                }
                "--model" => {
                    options.model = Some(args.next().ok_or("--model requires provider/model")?)
                }
                "--fixture-delay-ms" => {
                    options.fixture_delay_ms = args
                        .next()
                        .ok_or("--fixture-delay-ms requires milliseconds")?
                        .parse()?
                }
                "--help" | "-h" => {
                    eprintln!(
                        "pi-acp-server [--data-dir PATH] [--config-dir PATH] [--credentials PATH] [--model provider/model] [--fixture-provider] [--fixture-delay-ms N]"
                    );
                    std::process::exit(0)
                }
                _ => return Err(format!("Unknown option {arg}").into()),
            }
        }
        Ok(options)
    }
}
struct Spawner(tokio::runtime::Handle);
impl TaskSpawner for Spawner {
    fn spawn(&self, f: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.spawn(f);
        Ok(())
    }
}
pub struct Application {
    pub options: Options,
    pub store: Arc<dyn SessionStore>,
    pub registry: Option<Arc<ModelsRegistry>>,
    pub models: Vec<Model>,
    pub default_model: Model,
    pub runtime: RequestRuntime,
    pub settings: Value,
}
impl Application {
    pub async fn new(options: Options) -> Result<Self, AppError> {
        std::fs::create_dir_all(&options.data_dir)?;
        std::fs::create_dir_all(&options.config_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&options.data_dir, std::fs::Permissions::from_mode(0o700))?;
        }
        let handle = tokio::runtime::Handle::current();
        let clock: Arc<dyn Clock> = Arc::new(TokioClock::new(handle.clone()));
        let spawner: Arc<dyn TaskSpawner> = Arc::new(Spawner(handle.clone()));
        let runtime = RequestRuntime::with_noop_telemetry(
            CancellationToken::new(),
            clock.clone(),
            spawner.clone(),
        );
        let store = Arc::new(SqliteStore::open(options.data_dir.join("sessions.sqlite"))?);
        let mut settings =
            load_settings(&options.config_dir.join("settings.json"), None, &json!({})).values;
        if options.fixture_provider {
            settings["retry"] =
                json!({"enabled":true,"maxRetries":1,"baseDelayMs":5,"maxAgentDelayMs":10});
            settings["compaction"]["enabled"] = json!(false);
            settings["compaction"]["keepRecentTokens"] = json!(1);
        }
        let (registry, models) = if options.fixture_provider {
            (None, vec![fixture::model()])
        } else {
            let credentials = Arc::new(Credentials::open(
                &options.data_dir.join("credentials.sqlite"),
            )?);
            if let Some(path) = &options.credentials {
                credentials.import(path).await?;
            }
            let transport = Arc::new(HttpTransport::new(
                Arc::new(NativeHttpClient::new(handle.clone())),
                Arc::new(NativeRetryJitter),
            ));
            let oauth = OAuthServices {
                transport: transport.clone(),
                clock: clock.clone(),
                spawner: spawner.clone(),
                crypto: Arc::new(NativeOAuthCrypto),
                callbacks: Arc::new(NativeCallbackListener::new(handle)),
                callback_host: "127.0.0.1".into(),
                device_id: Some(installation_id(&options.data_dir)?),
                agent_name: Some("Pirua".into()),
                callback_limits: CallbackLimits::default(),
                max_token_response_bytes: 262144,
            };
            let registry = compose_registry(
                credentials,
                Arc::new(NativeProviderAuthContext::new(std::env::var("HOME").ok())),
                transport,
                &runtime,
                Some(oauth),
            )
            .await?;
            let models = registry.get_models(None);
            (Some(registry), models)
        };
        let default_model = if let Some(reference) = &options.model {
            models
                .iter()
                .find(|m| format!("{}/{}", m.provider, m.id) == *reference)
                .cloned()
                .ok_or_else(|| format!("Unknown selected model {reference}"))?
        } else if options.fixture_provider {
            models[0].clone()
        } else {
            let composition = ProviderComposition::new(ProviderCompositionInputs {
                models: models.clone(),
                ..Default::default()
            })?;
            composition
                .select_default_model()
                .cloned()
                .ok_or("No selected provider catalog is available")?
        };
        Ok(Self {
            options,
            store,
            registry,
            models,
            default_model,
            runtime,
            settings,
        })
    }
    pub fn resources(&self, cwd: &str) -> LoadedResources {
        load_resources(&ResourceLoaderOptions {
            cwd: PathBuf::from(cwd),
            agent_dir: self.options.config_dir.clone(),
            skill_paths: vec![],
            prompt_paths: vec![],
            include_defaults: true,
        })
    }
    pub async fn build(
        &self,
        manager: SessionManager,
    ) -> Result<(CodingSession, LoadedResources), AppError> {
        self.build_with_settings(manager, &self.settings).await
    }
    pub async fn build_with_settings(
        &self,
        manager: SessionManager,
        host_settings: &Value,
    ) -> Result<(CodingSession, LoadedResources), AppError> {
        let loaded = self.resources(&manager.header().cwd);
        let cwd = manager.header().cwd.clone();
        let settings = settings_snapshot(
            &self.default_model,
            host_settings,
            self.options.fixture_provider,
        )?;
        let builder = if let Some(registry) = &self.registry {
            CodingSessionBuilder::from_registry(
                manager,
                self.default_model.clone(),
                registry.clone(),
                self.runtime.clone(),
            )
        } else {
            CodingSessionBuilder::new(
                manager,
                self.default_model.clone(),
                fixture::stream(self.options.fixture_delay_ms),
            )
            .with_model_runtime(Arc::new(StaticModelRuntime::new(self.models.clone())))
            .with_hooks(Arc::new(fixture::Hooks))
        };
        let mut prompt = BuildSystemPromptOptions::new(
            cwd,
            DocumentationPaths {
                readme: String::new(),
                docs: String::new(),
                examples: String::new(),
            },
        );
        prompt.context_files = loaded.context_files.clone();
        let session = builder
            .with_system_prompt_options(prompt)
            .with_settings(settings)
            .with_resources(loaded.resources.clone())
            .with_image_policy(
                host_settings["images"]["autoResize"]
                    .as_bool()
                    .unwrap_or(true),
                host_settings["images"]["blockImages"]
                    .as_bool()
                    .unwrap_or(false),
            )
            .with_cache_warming(CacheWarmingMode::Off)
            .build()
            .await?;
        Ok((session, loaded))
    }
}
fn installation_id(path: &std::path::Path) -> Result<String, AppError> {
    let path = path.join("installation-id");
    match std::fs::read_to_string(&path) {
        Ok(v) => Ok(v),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let mut bytes = [0u8; 16];
            NativeOAuthCrypto.fill_random(&mut bytes)?;
            bytes[6] = (bytes[6] & 15) | 64;
            bytes[8] = (bytes[8] & 63) | 128;
            let h = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
            let id = format!(
                "{}-{}-{}-{}-{}",
                &h[..8],
                &h[8..12],
                &h[12..16],
                &h[16..20],
                &h[20..]
            );
            std::fs::write(path, &id)?;
            Ok(id)
        }
        Err(e) => Err(e.into()),
    }
}
fn settings_snapshot(
    model: &Model,
    v: &Value,
    fixture: bool,
) -> Result<CodingSettingsSnapshot, AppError> {
    let retry = if fixture {
        json!({"enabled":true,"maxRetries":1,"baseDelayMs":5,"maxAgentDelayMs":10})
    } else {
        json!({"enabled":v["retry"]["enabled"],"maxRetries":v["retry"]["maxRetries"],"baseDelayMs":v["retry"]["baseDelayMs"],"maxAgentDelayMs":v["retry"]["maxAgentDelayMs"]})
    };
    let mut compaction: CompactionSettings = serde_json::from_value(v["compaction"].clone())?;
    if fixture {
        compaction.enabled = false;
        compaction.keep_recent_tokens = SafeUnsigned::new(1)?;
    }
    Ok(CodingSettingsSnapshot::new(
        ModelRef::new(model.provider.clone(), model.id.clone()),
        serde_json::from_value(v.get("thinkingLevel").cloned().unwrap_or(json!("off")))?,
        ["read", "bash", "edit", "write"]
            .into_iter()
            .map(pi_ai::ToolName::new)
            .collect::<Result<Vec<_>, _>>()?,
        serde_json::from_value(retry)?,
        compaction,
        Default::default(),
        serde_json::from_value(v["steeringMode"].clone())?,
        serde_json::from_value(v["followUpMode"].clone())?,
    ))
}

/// The production composition path with explicit transport, credential and auth ports.
/// Tests supply deterministic ports here; the executable supplies native implementations.
pub async fn compose_registry(
    credentials: Arc<dyn CredentialStore>,
    auth_context: Arc<dyn AuthContext>,
    transport: Arc<HttpTransport>,
    runtime: &RequestRuntime,
    oauth: Option<OAuthServices>,
) -> Result<Arc<ModelsRegistry>, AppError> {
    let modules =
        pi_ai::builtins::BuiltinProviderModules::new(pi_ai::builtins::BuiltinProviderServices {
            anthropic: AdapterServices::new(transport.clone()),
            google: AdapterServices::new(transport.clone()),
            openai: AdapterServices::new(transport.clone()),
            completions: AdapterServices::new(transport),
            auth: pi_ai::builtins::FactoryAuthServices {
                spawner: runtime.spawner.clone(),
                oauth,
            },
        })?;
    Ok(Arc::new(
        pi_ai::builtins::builtin_models(
            &modules,
            ModelsRegistryOptions {
                identity: None,
                credentials,
                models_store: Box::new(InMemoryModelsStore::new()),
                auth_context,
                clock: runtime.clock.clone(),
                spawner: runtime.spawner.clone(),
                model_header_policy: ModelHeaderPolicy::default(),
            },
        )
        .await?,
    ))
}
