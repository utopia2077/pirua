//! Selected-model routing is separate from the physical model sent to a provider.
use pi_agent::{AgentError, BoxFuture, CancellationToken};
use pi_ai::{AssistantMessage, Message, Model, ModelThinkingLevel, SecretString};
use serde_json::Value;
use std::sync::Arc;

pub const VIRTUAL_MODEL_STATE_ENTRY: &str = "pi.virtual-model-state";
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelRouteReason {
    User,
    Continuation,
    Retry,
    Direct,
}
#[derive(Clone, Debug)]
pub struct ModelRouteRequest {
    pub selected_model: Model,
    pub messages: Vec<Message>,
    pub thinking_level: ModelThinkingLevel,
    pub reason: ModelRouteReason,
    pub failed: Option<RoutedFailure>,
    pub previous: Option<RoutedResponse>,
    pub state: Option<Value>,
    pub cancellation: CancellationToken,
}
#[derive(Clone, Debug)]
pub struct ModelRoute {
    pub model: Model,
    pub thinking_level: ModelThinkingLevel,
    pub state: Option<Value>,
}
#[derive(Clone, Debug)]
pub struct RoutedFailure {
    pub model: Model,
    pub thinking_level: Option<ModelThinkingLevel>,
    pub message: AssistantMessage,
}
#[derive(Clone, Debug)]
pub struct RoutedResponse {
    pub model: Model,
    pub thinking_level: Option<ModelThinkingLevel>,
}
/// Host-owned auth and dynamic routing capabilities. Default models route directly.
pub trait ModelRuntime: Send + Sync {
    fn get_model(&self, provider: &str, id: &str) -> Option<Model>;
    fn models(&self) -> Vec<Model>;
    fn is_virtual(&self, model: &Model) -> bool {
        model.api.as_str() == "pi-virtual"
    }
    fn resolve(&self, request: ModelRouteRequest) -> BoxFuture<'_, Result<ModelRoute, AgentError>> {
        Box::pin(async move {
            Ok(ModelRoute {
                model: request.selected_model,
                thinking_level: request.thinking_level,
                state: request.state,
            })
        })
    }
    fn api_key(
        &self,
        _model: Model,
        _signal: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<SecretString>, AgentError>> {
        Box::pin(async { Ok(None) })
    }
}
#[derive(Clone)]
pub struct StaticModelRuntime {
    models: Vec<Model>,
}
impl StaticModelRuntime {
    pub fn new(models: Vec<Model>) -> Self {
        Self { models }
    }
}
impl ModelRuntime for StaticModelRuntime {
    fn get_model(&self, provider: &str, id: &str) -> Option<Model> {
        self.models
            .iter()
            .find(|m| m.provider.as_str() == provider && m.id.as_str() == id)
            .cloned()
    }
    fn models(&self) -> Vec<Model> {
        self.models.clone()
    }
}
impl ModelRuntime for crate::ProviderComposition {
    fn get_model(&self, provider: &str, id: &str) -> Option<Model> {
        self.models()
            .find(|m| m.provider.as_str() == provider && m.id.as_str() == id)
            .cloned()
    }
    fn models(&self) -> Vec<Model> {
        self.models().cloned().collect()
    }
}
pub type ModelRuntimeHandle = Arc<dyn ModelRuntime>;

/// Restores virtual selections while physical responses retain their actual producing model.
pub fn branch_model_selection(
    branch: &[pi_coding_session::SessionEntry],
    runtime: &dyn ModelRuntime,
) -> Option<(String, String)> {
    use pi_coding_session::EntryContent;
    for (index, entry) in branch.iter().enumerate().rev() {
        match &entry.content {
            EntryContent::ModelChange { provider, model_id } => {
                return Some((provider.clone(), model_id.clone()));
            }
            EntryContent::Message { message }
                if message["role"] == "assistant" && message["api"] != "pi-virtual" =>
            {
                let response = (
                    message["provider"].as_str()?.to_owned(),
                    message["model"].as_str()?.to_owned(),
                );
                let change = branch[..index]
                    .iter()
                    .rev()
                    .find_map(|entry| match &entry.content {
                        EntryContent::ModelChange { provider, model_id } => {
                            Some((provider.clone(), model_id.clone()))
                        }
                        _ => None,
                    });
                return Some(
                    change
                        .filter(|(p, id)| {
                            runtime
                                .get_model(p, id)
                                .is_some_and(|m| runtime.is_virtual(&m))
                        })
                        .unwrap_or(response),
                );
            }
            _ => {}
        }
    }
    None
}
/// Latest state written by this virtual model on the active canonical branch.
pub fn branch_virtual_model_state(
    branch: &[pi_coding_session::SessionEntry],
    provider: &str,
    model_id: &str,
) -> Option<Value> {
    branch.iter().rev().find_map(|entry| match &entry.content {
        pi_coding_session::EntryContent::Custom {
            custom_type,
            data: Some(data),
        } if custom_type == VIRTUAL_MODEL_STATE_ENTRY
            && data["provider"] == provider
            && data["modelId"] == model_id =>
        {
            data.get("state").cloned()
        }
        _ => None,
    })
}
/// AI registry adapter. Authentication, request endpoints and provider capabilities remain AI-owned.
#[derive(Clone)]
pub struct RegistryModelRuntime {
    pub registry: Arc<pi_ai::ModelsRegistry>,
}
impl ModelRuntime for RegistryModelRuntime {
    fn get_model(&self, provider: &str, id: &str) -> Option<Model> {
        self.registry
            .get_models(None)
            .into_iter()
            .find(|m| m.provider.as_str() == provider && m.id.as_str() == id)
    }
    fn models(&self) -> Vec<Model> {
        self.registry.get_models(None)
    }
}
/// Converts a registry into an Agent stream without losing its transient request services.
pub fn registry_stream(
    registry: Arc<pi_ai::ModelsRegistry>,
    runtime: pi_ai::RequestRuntime,
) -> pi_agent::StreamFn {
    Arc::new(move |request| {
        let registry = registry.clone();
        let mut runtime = runtime.clone();
        Box::pin(async move {
            runtime.cancellation = request.cancellation;
            Ok(registry.stream_simple(
                request.model,
                pi_ai::Context {
                    system_prompt: None,
                    messages: request.context.into_messages(),
                    tools: None,
                },
                pi_ai::ModelsRequestOverrides {
                    auth: pi_ai::AuthResolutionOverrides {
                        api_key: request.api_key,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                pi_ai::SimpleStreamOptions {
                    wire: request.options,
                    runtime,
                },
            ))
        })
    })
}
