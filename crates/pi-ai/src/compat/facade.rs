//! Owned compatibility dispatch, with an explicitly captured environment authority.
use super::{
    CodexSessionResources, CompatApiBinding, CompatApiModules, CompatApiRegistry, CompatImages,
    CompatSharedApiModules, LegacyApiAliases,
};
use crate::builtins::{BuiltinCompositionError, BuiltinProviderModules, builtin_models};
use pi_ai_core::*;
use pi_ai_providers::{
    api::lazy::lazy_stream,
    auth::get_env_api_key,
    images::{ImagesApiRegistry, ImagesGenerateCall},
};
use std::{fmt, sync::Arc};
enum Target {
    Provider(Arc<dyn ApiAdapter>),
    Binding(Arc<CompatApiBinding>),
}
impl Target {
    fn stream(self, r: TextStreamRequest) -> Result<AssistantStream, ModelsError> {
        match self {
            Self::Provider(a) => Ok(a.stream(r)),
            Self::Binding(a) => a.stream(r),
        }
    }
    fn simple(self, r: SimpleStreamRequest) -> Result<AssistantStream, ModelsError> {
        match self {
            Self::Provider(a) => Ok(a.stream_simple(r)),
            Self::Binding(a) => a.stream_simple(r),
        }
    }
}
pub struct CompatFacade {
    registry: CompatApiRegistry,
    models: ModelsRegistry,
    modules: Arc<CompatApiModules>,
    aliases: LegacyApiAliases,
    images: CompatImages,
    auth_context: Arc<dyn AuthContext>,
    resources: CodexSessionResources,
}
impl CompatFacade {
    pub fn initialize(
        registry: CompatApiRegistry,
        modules: Arc<BuiltinProviderModules>,
        shared: CompatSharedApiModules,
        options: ModelsRegistryOptions,
    ) -> BoxFuture<'static, Result<Self, BuiltinCompositionError>> {
        Box::pin(async move {
            let auth_context = options.auth_context.clone();
            let api_modules = Arc::new(CompatApiModules::new(modules.clone(), shared));
            registry.register_builtin_api_providers(&api_modules);
            let images = CompatImages::new(&modules);
            images.register_builtin_images_api_providers();
            let models = builtin_models(&modules, options).await?;
            let aliases = LegacyApiAliases::new(api_modules.clone());
            let resources = CodexSessionResources::new(modules.codex());
            Ok(Self {
                registry,
                models,
                modules: api_modules,
                aliases,
                images,
                auth_context,
                resources,
            })
        })
    }
    pub fn registry(&self) -> &CompatApiRegistry {
        &self.registry
    }
    pub fn modules(&self) -> &Arc<CompatApiModules> {
        &self.modules
    }
    pub fn aliases(&self) -> &LegacyApiAliases {
        &self.aliases
    }
    pub fn session_resources(&self) -> &CodexSessionResources {
        &self.resources
    }
    pub fn images_registry(&self) -> &ImagesApiRegistry {
        self.images.registry()
    }
    pub fn register_builtin_images_api_providers(&self) {
        self.images.register_builtin_images_api_providers()
    }
    pub fn generate_images_openrouter(
        &self,
        request: ImageGenerationRequest,
    ) -> ImagesGenerateCall<'_> {
        self.images.generate_images_openrouter(request)
    }
    pub fn generate_images(
        &self,
        request: ImageGenerationRequest,
    ) -> BoxFuture<'_, Result<AssistantImages, ModelsError>> {
        self.images.generate_images(request)
    }
    fn capture(&self, model: &Model) -> Result<Target, ModelsError> {
        let (binding, remembered) = self
            .registry
            .capture(&model.api)
            .ok_or_else(|| ModelsError::new(ModelsErrorCode::Provider))?;
        binding.check(&model.api)?;
        if remembered
            && let Some(provider) = self.models.get_provider(&model.provider)
            && provider.models()?.iter().any(|m| m.api == model.api)
        {
            return provider
                .adapter(&model.api)
                .map(Target::Provider)
                .ok_or_else(|| ModelsError::new(ModelsErrorCode::Provider));
        }
        Ok(Target::Binding(binding))
    }
    pub fn stream(&self, mut request: TextStreamRequest) -> Result<AssistantStream, ModelsError> {
        let target = self.capture(&request.resolved.model)?;
        if explicit(&request.auth) {
            return target.stream(request);
        }
        let init = init(&request.resolved.model, &request.options.runtime);
        let spawner = request.options.runtime.spawner.clone();
        let context = self.auth_context.clone();
        Ok(lazy_stream(init, spawner.as_ref(), move || {
            Box::pin(async move {
                inject(
                    &mut request.auth,
                    &request.resolved.model.provider,
                    context.as_ref(),
                    &request.options.runtime.cancellation,
                )
                .await?;
                target.stream(request)
            })
        }))
    }
    pub fn stream_simple(
        &self,
        mut request: SimpleStreamRequest,
    ) -> Result<AssistantStream, ModelsError> {
        let target = self.capture(&request.resolved.model)?;
        if explicit(&request.auth) {
            return target.simple(request);
        }
        let init = init(&request.resolved.model, &request.options.runtime);
        let spawner = request.options.runtime.spawner.clone();
        let context = self.auth_context.clone();
        Ok(lazy_stream(init, spawner.as_ref(), move || {
            Box::pin(async move {
                inject(
                    &mut request.auth,
                    &request.resolved.model.provider,
                    context.as_ref(),
                    &request.options.runtime.cancellation,
                )
                .await?;
                target.simple(request)
            })
        }))
    }
    pub fn complete(
        &self,
        request: TextStreamRequest,
    ) -> BoxFuture<'static, Result<AssistantMessage, ModelsError>> {
        let stream = self.stream(request);
        Box::pin(async move { Ok(stream?.result().await) })
    }
    pub fn complete_simple(
        &self,
        request: SimpleStreamRequest,
    ) -> BoxFuture<'static, Result<AssistantMessage, ModelsError>> {
        let stream = self.stream_simple(request);
        Box::pin(async move { Ok(stream?.result().await) })
    }
}
fn init(model: &Model, runtime: &RequestRuntime) -> AssistantStreamInit {
    AssistantStreamInit {
        api: model.api.clone(),
        provider: model.provider.clone(),
        model: model.id.clone(),
        clock: runtime.clock.clone(),
        cancellation: runtime.cancellation.clone(),
    }
}
fn whitespace(c: char) -> bool {
    matches!(c,'\u{9}'..='\u{d}'|' '| '\u{a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}'|'\u{feff}')
}
fn explicit(auth: &ResolvedAuth) -> bool {
    auth.api_key
        .as_ref()
        .is_some_and(|v| !v.expose_secret().trim_matches(whitespace).is_empty())
}
async fn inject(
    auth: &mut ResolvedAuth,
    provider: &ProviderId,
    context: &dyn AuthContext,
    c: &CancellationToken,
) -> Result<(), ModelsError> {
    let key = get_env_api_key(provider.as_str(), Some(&auth.env), context, c)
        .await
        .map_err(|e| ModelsError::with_source(ModelsErrorCode::Auth, e))?;
    if let Some(key) = key
        && !key.expose_secret().is_empty()
        && key.expose_secret() != "<authenticated>"
    {
        auth.api_key = Some(key);
    }
    Ok(())
}
impl fmt::Debug for CompatFacade {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CompatFacade([REDACTED])")
    }
}
