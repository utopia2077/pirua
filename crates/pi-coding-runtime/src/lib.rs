//! Headless coding tools and composition over the AI and Agent contracts.

pub mod credentials;
pub mod models;
pub mod provider_composition;
pub mod resources;
pub mod settings_snapshot;
pub mod shell_context;
pub mod system_prompt;
pub mod tools;

pub use credentials::CredentialInputs;
pub use models::{
    CatalogLookupError, EntranceMetadata, builtin_model, builtin_models, default_thinking_level,
    is_selected_text_provider, selected_entrances, selected_text_entrances,
    selected_text_provider_ids,
};
pub use provider_composition::{
    CodingRuntimeError, ConfiguredModelOverlay, ModelAvailability, ModelReferenceRequest,
    ModelReferenceResult, ModelReferenceTarget, ProviderComposition, ProviderCompositionInputs,
    ResolvedModel, ThinkingSuffix, resolve_model_reference,
};
pub use resources::{
    ContextFile, LoadedResources, PromptTemplate, ResourceDiagnostic, ResourceLoaderOptions,
    ResourceNormalizationError, Resources, Skill, SkillFileReader, expand_prompt_template,
    format_skills_for_prompt, format_skills_for_prompt_with_reader, load_project_context_files,
    load_resources, normalize_resources, parse_command_args, parse_frontmatter, substitute_args,
};
pub use settings_snapshot::{
    CodingSettingsSnapshot, CompactionSettings, LoadedSettings, ModelRef, QueueMode,
    SettingsDiagnostic, ThinkingLevel, headless_settings_defaults, load_settings, merge_settings,
};
pub use shell_context::{ShellIdentity, with_shell_identity};
pub use system_prompt::{
    BuildSystemPromptOptions, DocumentationPaths, SystemPromptError, SystemPromptState,
    ToolPromptMetadata, build_coding_agent_system_prompt, build_system_prompt,
    build_system_prompt_sections, build_system_prompt_state, diff_system_prompt_sections,
};
pub use tools::{
    CodingTool, EnvError, EnvResult, ExecutionEnv, ExecutionToolContext, FileInfo,
    LocalExecutionEnv, ProcessData, ProcessOptions, ProcessOutput, ProcessedImage, ReadToolOptions,
    SearchOptions, ShellToolOptions, TOOL_NAMES, ToolOutput, ToolOutputUpdate, Truncation,
    bind_agent_tool, coding_tool, create_bash_tool, create_bash_tool_with_options,
    create_edit_tool, create_find_tool, create_grep_tool, create_ls_tool, create_powershell_tool,
    create_powershell_tool_with_options, create_read_tool, create_read_tool_with_options,
    create_tool, create_tools, create_write_tool, read_operation, shell_operation,
    tool_prompt_metadata,
};

#[cfg(test)]
extern crate self as pi_coding_runtime;
#[cfg(test)]
#[path = "../tests/settings_snapshot.rs"]
mod settings_tests;
#[cfg(test)]
#[path = "../tests/tools.rs"]
mod tool_tests;

#[cfg(test)]
#[path = "../tests/target_tools.rs"]
mod target_tools_tests;

#[cfg(test)]
#[path = "../tests/model_composition.rs"]
mod model_composition_tests;
#[cfg(test)]
#[path = "../tests/model_defaults.rs"]
mod model_default_tests;

#[cfg(test)]
#[path = "../tests/local_boundaries.rs"]
mod local_boundary_tests;
