//! Configured modules and typed raw/simple legacy calls. No auth or discovery.
use super::CompatApiBinding;
use crate::builtins::BuiltinProviderModules;
use pi_ai_core::*;
use pi_ai_providers::api::{
    anthropic_messages::*, azure_openai_responses::*, bedrock_converse_stream::*, google_genai::*,
    google_vertex::*, lazy::*, mistral_conversations::*, openai_codex_responses::*,
    openai_completions::*, openai_responses::*, pi_messages::*,
};
use std::{fmt, sync::Arc};
#[derive(Clone)]
pub struct CompatSharedApiModules {
    pub azure: Arc<AzureOpenAIResponsesApi>,
    pub google_vertex: Arc<GoogleVertexApi>,
    pub mistral: Arc<MistralApi>,
    pub bedrock: Arc<BedrockApi>,
    pub pi_messages: Arc<PiMessagesApi>,
}
struct FixedLoader(Arc<dyn ApiAdapter>);
impl LazyApiLoader for FixedLoader {
    fn load(self: Arc<Self>) -> BoxFuture<'static, Result<Arc<dyn ApiAdapter>, ModelsError>> {
        Box::pin(async move { Ok(self.0.clone()) })
    }
}
fn fixed(adapter: Arc<dyn ApiAdapter>) -> Arc<LazyApi> {
    let id = adapter.api_id().clone();
    Arc::new(lazy_api(
        id,
        Arc::new(FixedLoader(adapter)),
        LazyApiCapabilities::default(),
    ))
}
/// Retains one configured adapter and one lazy wrapper for each registered family.
pub struct CompatApiModules {
    pub(super) selected: Arc<BuiltinProviderModules>,
    pub(super) shared: CompatSharedApiModules,
    pub(super) apis: Vec<Arc<LazyApi>>,
}
impl CompatApiModules {
    pub fn new(selected: Arc<BuiltinProviderModules>, shared: CompatSharedApiModules) -> Self {
        let apis = vec![
            fixed(selected.anthropic()),
            fixed(selected.completions()),
            fixed(selected.openai()),
            fixed(selected.codex()),
            fixed(shared.azure.clone()),
            fixed(selected.google()),
            fixed(shared.google_vertex.clone()),
            fixed(shared.mistral.clone()),
            Arc::new(bedrock_converse_stream_api(Arc::new(FixedLoader(
                shared.bedrock.clone(),
            )))),
            fixed(shared.pi_messages.clone()),
        ];
        Self {
            selected,
            shared,
            apis,
        }
    }
    pub(super) fn bindings(&self) -> Vec<Arc<CompatApiBinding>> {
        self.apis
            .iter()
            .map(|a| Arc::new(CompatApiBinding::new(a.clone(), None)))
            .collect()
    }
    pub fn codex(&self) -> Arc<OpenAICodexResponsesApi> {
        self.selected.codex()
    }
}
pub struct LegacyApiAliases {
    modules: Arc<CompatApiModules>,
}
impl LegacyApiAliases {
    pub fn new(modules: Arc<CompatApiModules>) -> Self {
        Self { modules }
    }
    /// Direct configured Bedrock module, retaining typed raw options. The registered
    /// compatibility wrapper separately applies global override/fallback.
    pub fn bedrock_provider_module(&self) -> Arc<BedrockApi> {
        self.modules.shared.bedrock.clone()
    }
}
fn typed_raw<T: ApiAdapter + 'static, O: Send + 'static>(
    module: Arc<T>,
    request: TextStreamRequest,
    options: O,
    call: fn(&T, TextStreamRequest, O) -> AssistantStream,
) -> AssistantStream {
    let init = AssistantStreamInit {
        api: request.resolved.model.api.clone(),
        provider: request.resolved.model.provider.clone(),
        model: request.resolved.model.id.clone(),
        clock: request.options.runtime.clock.clone(),
        cancellation: request.options.runtime.cancellation.clone(),
    };
    let spawner = request.options.runtime.spawner.clone();
    lazy_stream(init, spawner.as_ref(), move || {
        Box::pin(async move { Ok(call(module.as_ref(), request, options)) })
    })
}
macro_rules! alias {
    ($raw:ident,$simple:ident,$ty:ty,$options:ty,$get:expr,$index:expr) => {
        impl LegacyApiAliases {
            pub fn $raw(&self, request: TextStreamRequest, options: $options) -> AssistantStream {
                let module: Arc<$ty> = ($get)(&self.modules);
                typed_raw(module, request, options, <$ty>::stream_with_options)
            }
            pub fn $simple(&self, request: SimpleStreamRequest) -> AssistantStream {
                self.modules.apis[$index].stream_simple(request)
            }
        }
    };
}
alias!(
    stream_anthropic,
    stream_simple_anthropic,
    AnthropicMessagesApi,
    AnthropicOptions,
    |m: &CompatApiModules| m.selected.anthropic(),
    0
);
alias!(
    stream_azure_openai_responses,
    stream_simple_azure_openai_responses,
    AzureOpenAIResponsesApi,
    AzureOpenAIResponsesOptions,
    |m: &CompatApiModules| m.shared.azure.clone(),
    4
);
alias!(
    stream_google,
    stream_simple_google,
    GoogleGenAiApi,
    GoogleOptions,
    |m: &CompatApiModules| m.selected.google(),
    5
);
alias!(
    stream_google_vertex,
    stream_simple_google_vertex,
    GoogleVertexApi,
    GoogleVertexOptions,
    |m: &CompatApiModules| m.shared.google_vertex.clone(),
    6
);
alias!(
    stream_mistral,
    stream_simple_mistral,
    MistralApi,
    MistralOptions,
    |m: &CompatApiModules| m.shared.mistral.clone(),
    7
);
alias!(
    stream_openai_codex_responses,
    stream_simple_openai_codex_responses,
    OpenAICodexResponsesApi,
    OpenAICodexResponsesOptions,
    |m: &CompatApiModules| m.selected.codex(),
    3
);
alias!(
    stream_openai_completions,
    stream_simple_openai_completions,
    OpenAICompletionsApi,
    OpenAICompletionsOptions,
    |m: &CompatApiModules| m.selected.completions(),
    1
);
alias!(
    stream_openai_responses,
    stream_simple_openai_responses,
    OpenAIResponsesApi,
    OpenAIResponsesOptions,
    |m: &CompatApiModules| m.selected.openai(),
    2
);
macro_rules! debug {($($ty:ty),*)=>{$(impl fmt::Debug for $ty{fn fmt(&self,f:&mut fmt::Formatter<'_>)->fmt::Result{f.write_str(concat!(stringify!($ty),"([REDACTED])"))}})*};}
debug!(CompatSharedApiModules, CompatApiModules, LegacyApiAliases);
