//! Headless CodingSession composition. No provider runs during construction.
use crate::{
    BuildSystemPromptOptions, CodingSession, CodingSettingsSnapshot, DocumentationPaths,
    ExecutionToolContext, LocalExecutionEnv, ModelRuntime, NoopSessionHooks, RegisteredTool,
    Resources, SessionError, SessionHooks, StaticModelRuntime, ToolExposure,
};
use pi_agent::{AgentLoopConfig, StreamFn};
use pi_ai::Model;
use pi_coding_session::SessionManager;
use std::sync::Arc;

pub struct CodingSessionBuilder {
    pub(crate) manager: SessionManager,
    pub(crate) config: AgentLoopConfig,
    pub(crate) settings: Option<CodingSettingsSnapshot>,
    pub(crate) thinking_level: Option<pi_ai::ModelThinkingLevel>,
    pub(crate) resources: Resources,
    pub(crate) hooks: Arc<dyn SessionHooks>,
    pub(crate) model_runtime: Arc<dyn ModelRuntime>,
    pub(crate) tools: Vec<RegisteredTool>,
    pub(crate) prompt_options: BuildSystemPromptOptions,
    pub(crate) cache_warming: crate::CacheWarmingMode,
    pub(crate) execution_env: Arc<dyn crate::ExecutionEnv>,
    pub(crate) uses_default_tools: bool,
    pub(crate) local_environment: bool,
    pub(crate) image_auto_resize: bool,
    pub(crate) block_images: bool,
}
impl CodingSessionBuilder {
    pub fn new(manager: SessionManager, model: Model, stream: StreamFn) -> Self {
        let cwd = manager.header().cwd.clone();
        let execution_env: Arc<dyn crate::ExecutionEnv> = Arc::new(LocalExecutionEnv::new(&cwd));
        let context = ExecutionToolContext {
            env: execution_env.clone(),
        };
        let tools = crate::create_tools(context)
            .into_iter()
            .map(|tool| RegisteredTool {
                tool,
                exposure: ToolExposure::Direct,
                source: Some("builtin".into()),
                default_active: true,
                namespace: None,
                prompt_snippet: None,
                prompt_guidelines: Vec::new(),
            })
            .collect();
        Self {
            manager,
            config: AgentLoopConfig::new(model.clone(), stream),
            settings: None,
            thinking_level: None,
            resources: Resources::default(),
            hooks: Arc::new(NoopSessionHooks),
            model_runtime: Arc::new(StaticModelRuntime::new(vec![model])),
            tools,
            prompt_options: BuildSystemPromptOptions::new(
                cwd,
                DocumentationPaths {
                    readme: String::new(),
                    docs: String::new(),
                    examples: String::new(),
                },
            ),
            cache_warming: crate::CacheWarmingMode::Streaming,
            execution_env,
            uses_default_tools: true,
            local_environment: true,
            image_auto_resize: true,
            block_images: false,
        }
    }
    pub fn from_registry(
        manager: SessionManager,
        model: Model,
        registry: Arc<pi_ai::ModelsRegistry>,
        runtime: pi_ai::RequestRuntime,
    ) -> Self {
        Self::new(
            manager,
            model,
            crate::registry_stream(registry.clone(), runtime),
        )
        .with_model_runtime(Arc::new(crate::RegistryModelRuntime { registry }))
    }
    pub fn with_thinking_level(mut self, level: pi_ai::ModelThinkingLevel) -> Self {
        self.thinking_level = Some(level);
        self
    }
    pub fn with_settings(mut self, settings: CodingSettingsSnapshot) -> Self {
        self.settings = Some(settings);
        self
    }
    pub fn with_resources(mut self, resources: Resources) -> Self {
        self.resources = resources;
        self
    }
    pub fn with_hooks(mut self, hooks: Arc<dyn SessionHooks>) -> Self {
        self.hooks = hooks;
        self
    }
    pub fn with_model_runtime(mut self, runtime: Arc<dyn ModelRuntime>) -> Self {
        self.model_runtime = runtime;
        self
    }
    pub fn with_tools(mut self, tools: Vec<pi_agent::AgentTool>) -> Self {
        self.uses_default_tools = false;
        self.tools = tools.into_iter().map(RegisteredTool::direct).collect();
        self
    }
    pub fn with_registered_tools(mut self, tools: Vec<RegisteredTool>) -> Self {
        self.uses_default_tools = false;
        self.tools = tools;
        self
    }
    pub fn with_tool_context(mut self, context: ExecutionToolContext) -> Self {
        self.execution_env = context.env.clone();
        self.local_environment = false;
        self.uses_default_tools = true;
        self.tools = crate::create_tools(context)
            .into_iter()
            .map(|tool| {
                let mut registered = RegisteredTool::direct(tool);
                registered.source = Some("builtin".into());
                registered
            })
            .collect();
        self
    }
    pub fn with_system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.prompt_options.force_system_prompt = Some(prompt.into());
        self
    }
    pub fn with_system_prompt_options(mut self, options: BuildSystemPromptOptions) -> Self {
        self.prompt_options = options;
        self
    }
    pub fn with_loop_config(mut self, config: AgentLoopConfig) -> Self {
        self.config = config;
        self
    }
    pub fn with_image_policy(mut self, auto_resize: bool, block_images: bool) -> Self {
        self.image_auto_resize = auto_resize;
        self.block_images = block_images;
        self
    }
    pub fn with_cache_warming(mut self, mode: crate::CacheWarmingMode) -> Self {
        self.cache_warming = mode;
        self
    }
    pub async fn build(self) -> Result<CodingSession, SessionError> {
        CodingSession::build(self).await
    }
}
