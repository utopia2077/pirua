//! Private headless coding-runtime composition crate for the Rust harness.

pub mod credentials;
pub mod tools;
pub mod messages;
pub mod models;
pub mod provider_composition;
pub mod resources;
pub mod settings_snapshot;
pub mod system_prompt;

pub use credentials::CredentialInputs;
pub use messages::CodingMessageConverter;
pub use models::{
    CatalogLookupError, EntranceKind, EntranceMetadata, builtin_image_model, builtin_image_models,
    builtin_model, builtin_models, is_selected_image_entrance, is_selected_text_provider,
    selected_entrances, selected_image_entrances, selected_text_entrances,
    selected_text_provider_ids,
};
pub use provider_composition::{
    CodingRuntimeError, ProviderComposition, ProviderCompositionInputs,
};
pub use resources::{
    ContextFile, PromptTemplate, ResourceNormalizationError, Resources, Skill,
    format_skills_for_prompt, normalize_resources,
};
pub use settings_snapshot::{
    CodingSettingsSnapshot, CompactionSettings, ModelRef, QueueMode, ThinkingLevel,
};
pub use system_prompt::{
    BuildSystemPromptOptions, DocumentationPaths, ToolPromptMetadata,
    build_coding_agent_harness_system_prompt, build_system_prompt,
};
