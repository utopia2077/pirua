//! Private headless coding-runtime composition crate for the Rust harness.

pub mod credentials;
pub mod models;
pub mod provider_composition;
pub mod settings_snapshot;

pub use credentials::CredentialInputs;
pub use models::{
    CatalogLookupError, EntranceKind, EntranceMetadata, builtin_image_model, builtin_image_models,
    builtin_model, builtin_models, is_selected_image_entrance, is_selected_text_provider,
    selected_entrances, selected_image_entrances, selected_text_entrances,
    selected_text_provider_ids,
};
pub use provider_composition::{
    CodingRuntimeError, ProviderComposition, ProviderCompositionInputs,
};
pub use settings_snapshot::{
    CodingSettingsSnapshot, CompactionSettings, ModelRef, QueueMode, ThinkingLevel,
};
