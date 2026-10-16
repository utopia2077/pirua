//! Detached coding-runtime settings captured for later Agent composition.
//!
//! The runtime receives these values from its host.  It does not read settings
//! files, persist credentials, or perform provider/network work while creating
//! a snapshot. The optional local loader returns detached configuration and
//! diagnostics. IDs, stream options, retry policy and thinking levels come from
//! AI; queue mode comes from Agent schema, while Coding owns compaction settings.

use pi_ai::{ModelId, ModelThinkingLevel, ProviderId, RetryPolicy, StreamOptionsWire, ToolName};

/// A provider-scoped model identity selected for a coding run.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ModelRef {
    /// Stable provider identity.
    pub provider: ProviderId,
    /// Provider-scoped model identity.
    pub model_id: ModelId,
}

impl ModelRef {
    /// Creates a model reference from validated provider and model identities.
    pub const fn new(provider: ProviderId, model_id: ModelId) -> Self {
        Self { provider, model_id }
    }
}

/// The public model thinking level, including disabled thinking.
pub type ThinkingLevel = ModelThinkingLevel;

pub use pi_agent_schema::QueueMode;

/// Coding compaction configuration, independent of either runtime.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactionSettings {
    pub enabled: bool,
    pub reserve_tokens: pi_ai::SafeUnsigned,
    pub keep_recent_tokens: pi_ai::SafeUnsigned,
}
impl Default for CompactionSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            reserve_tokens: pi_ai::SafeUnsigned::new(16_384).expect("constant reserve"),
            keep_recent_tokens: pi_ai::SafeUnsigned::new(20_000).expect("constant tail"),
        }
    }
}

/// Detached normalized settings required by the later coding Agent builder.
///
/// This type contains no handles to a provider, credential store, session
/// writer, filesystem, process, or network client.  A builder may therefore
/// clone it at a composition boundary and apply it at an Agent composition boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct CodingSettingsSnapshot {
    /// Provider-scoped model selected for the next run.
    pub model: ModelRef,
    /// Requested model thinking level.
    pub thinking_level: ThinkingLevel,
    /// Model-visible coding tools in host-supplied order.
    pub active_tools: Vec<ToolName>,
    /// Normalized provider retry policy.
    pub retry: RetryPolicy,
    /// Compaction thresholds for this run.
    pub compaction: CompactionSettings,
    /// Provider-neutral stream options.
    pub stream: StreamOptionsWire,
    /// Steering queue drain mode.
    pub steering_mode: QueueMode,
    /// Follow-up queue drain mode.
    pub follow_up_mode: QueueMode,
}

impl CodingSettingsSnapshot {
    /// Captures already-normalized host values without side effects.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        model: ModelRef,
        thinking_level: ThinkingLevel,
        active_tools: Vec<ToolName>,
        retry: RetryPolicy,
        compaction: CompactionSettings,
        stream: StreamOptionsWire,
        steering_mode: QueueMode,
        follow_up_mode: QueueMode,
    ) -> Self {
        Self {
            model,
            thinking_level,
            active_tools,
            retry,
            compaction,
            stream,
            steering_mode,
            follow_up_mode,
        }
    }
}

/// Source-aware diagnostics produced while reading local settings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettingsDiagnostic {
    pub path: std::path::PathBuf,
    pub message: String,
}
/// A detached, recursively merged settings document. Unknown host settings survive.
#[derive(Clone, Debug, PartialEq)]
pub struct LoadedSettings {
    pub values: serde_json::Value,
    pub diagnostics: Vec<SettingsDiagnostic>,
}
/// Selected headless defaults from v1.4.0 settings-defaults.ts.
pub fn headless_settings_defaults() -> serde_json::Value {
    serde_json::json!({
        "transport":"auto", "steeringMode":"one-at-a-time", "followUpMode":"one-at-a-time",
        "compaction":{"enabled":true,"reserveTokens":16384,"keepRecentTokens":20000},
        "branchSummary":{"reserveTokens":16384,"skipPrompt":false},
        "retry":{"enabled":true,"maxRetries":3,"baseDelayMs":2000,"maxAgentDelayMs":60000,"provider":{"maxRetryDelayMs":60000}},
        "images":{"autoResize":true,"blockImages":false}, "enableSkillCommands":true,
        "codemode":{"mode":"on","inlineBudget":3000}, "cacheWarming":"streaming"
    })
}
/// Merge objects recursively; arrays and scalar values replace their predecessor.
pub fn merge_settings(
    base: &serde_json::Value,
    overrides: &serde_json::Value,
) -> serde_json::Value {
    match (base.as_object(), overrides.as_object()) {
        (Some(base), Some(overrides)) => {
            let mut result = base.clone();
            for (name, value) in overrides {
                let merged = result
                    .get(name)
                    .map_or_else(|| value.clone(), |base| merge_settings(base, value));
                result.insert(name.clone(), merged);
            }
            serde_json::Value::Object(result)
        }
        _ => overrides.clone(),
    }
}
/// Read global then optional host-authorized project settings and apply runtime overrides.
/// No credential, trust, network or persistence actions occur during this snapshot.
pub fn load_settings(
    global_path: &std::path::Path,
    project_path: Option<&std::path::Path>,
    overrides: &serde_json::Value,
) -> LoadedSettings {
    let mut result = LoadedSettings {
        values: headless_settings_defaults(),
        diagnostics: Vec::new(),
    };
    for path in std::iter::once(global_path).chain(project_path) {
        match std::fs::read_to_string(path) {
            Ok(text) => match serde_json::from_str::<serde_json::Value>(
                text.strip_prefix('\u{feff}').unwrap_or(&text),
            ) {
                Ok(value) if value.is_object() => {
                    result.values = merge_settings(&result.values, &value)
                }
                Ok(_) => result.diagnostics.push(SettingsDiagnostic {
                    path: path.into(),
                    message: "settings must be an object".into(),
                }),
                Err(error) => result.diagnostics.push(SettingsDiagnostic {
                    path: path.into(),
                    message: error.to_string(),
                }),
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => result.diagnostics.push(SettingsDiagnostic {
                path: path.into(),
                message: error.to_string(),
            }),
        }
    }
    if overrides.is_object() {
        result.values = merge_settings(&result.values, overrides);
    } else {
        result.diagnostics.push(SettingsDiagnostic {
            path: std::path::PathBuf::from("<runtime>"),
            message: "settings overrides must be an object".into(),
        });
    }
    result
}
