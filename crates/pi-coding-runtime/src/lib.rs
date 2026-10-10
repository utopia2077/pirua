//! Private headless coding-runtime composition crate for the Rust harness.

pub mod builder;
pub mod credentials;
pub mod legacy_import;
pub mod messages;
pub mod models;
pub mod provider_composition;
pub mod resources;
pub mod session_lifecycle;
pub mod settings_snapshot;
pub mod system_prompt;
pub mod tools;

pub use builder::{CodingHarness, CodingHarnessBuilder, CodingHarnessError};
pub use credentials::CredentialInputs;
pub use legacy_import::{LegacyImportSource, open_external_jsonl};
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
pub use session_lifecycle::CodingSessionLifecycle;
pub use settings_snapshot::{
    CodingSettingsSnapshot, CompactionSettings, ModelRef, QueueMode, ThinkingLevel,
};
pub use system_prompt::{
    BuildSystemPromptOptions, DocumentationPaths, ToolPromptMetadata,
    build_coding_agent_harness_system_prompt, build_system_prompt,
};
pub use tools::{
    EnvError, ExecutionEnv, ExecutionToolContext, FileInfo, LocalExecutionEnv, ProcessOptions,
    ProcessOutput, ProcessedImage, TOOL_NAMES, create_bash_tool, create_edit_tool,
    create_find_tool, create_grep_tool, create_ls_tool, create_powershell_tool, create_read_tool,
    create_tool, create_tools, create_write_tool, tool_prompt_metadata,
};
