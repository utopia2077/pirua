//! Selected coding-runtime entrances and immutable catalog lookup.
//!
//! The catalog functions are thin wrappers over the captured `pi-ai` catalogs.
//! They do not refresh catalogs, inspect credentials, or access a network.

use pi_ai::{Model, ModelThinkingLevel};
use std::fmt;

// Selected text defaults, preserving their relative order in the pinned
// pi-mono 4a080252f06caf63a525d7873c5d94d3c23887e0 model-resolver.ts table.
pub(crate) const DEFAULT_MODEL_PER_PROVIDER: [(&str, &str); 4] = [
    ("anthropic", "claude-opus-4-8"),
    ("openai", "gpt-5.5"),
    ("deepseek", "deepseek-v4-pro"),
    ("google", "gemini-3.1-pro-preview"),
];

/// Returns the coding product's pinned default thinking preference.
///
/// Hosts may use this preference when normalizing their settings. It does not
/// change explicit model selection, validate model thinking support, or replace
/// `resolve_selection`'s Off behavior when no thinking suffix is supplied.
pub const fn default_thinking_level() -> ModelThinkingLevel {
    ModelThinkingLevel::Medium
}

/// Stable metadata for one selected builtin provider entrance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntranceMetadata {
    /// Public builtin provider entrance identity.
    pub id: &'static str,
    /// Provider identity used by the underlying `pi-ai` contract.
    pub provider: &'static str,
}

const TEXT_ENTRANCES: [EntranceMetadata; 4] = [
    EntranceMetadata {
        id: "openai",
        provider: "openai",
    },
    EntranceMetadata {
        id: "google",
        provider: "google",
    },
    EntranceMetadata {
        id: "anthropic",
        provider: "anthropic",
    },
    EntranceMetadata {
        id: "deepseek",
        provider: "deepseek",
    },
];

/// Returns the four selected text entrances in stable product order.
pub const fn selected_text_entrances() -> &'static [EntranceMetadata] {
    &TEXT_ENTRANCES
}

/// Returns all four builtin entrances in stable product order.
pub fn selected_entrances() -> Vec<EntranceMetadata> {
    TEXT_ENTRANCES.to_vec()
}

/// Returns the selected text provider identities in stable product order.
pub fn selected_text_provider_ids() -> impl ExactSizeIterator<Item = &'static str> {
    TEXT_ENTRANCES.iter().map(|entry| entry.provider)
}

/// Returns whether an identity is one of the selected text providers.
pub fn is_selected_text_provider(provider: &str) -> bool {
    TEXT_ENTRANCES
        .iter()
        .any(|entry| entry.provider == provider)
}

/// Error while admitting a captured catalog result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogLookupError {
    /// A captured catalog failed validation in `pi-ai`.
    InvalidCatalog,
}

impl fmt::Display for CatalogLookupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("captured catalog lookup failed")
    }
}

impl std::error::Error for CatalogLookupError {}

/// Returns the captured text catalog for one selected provider.
///
/// If the corresponding opt-in feature is disabled, the result is an empty
/// catalog. This mirrors `pi-ai`'s feature-selected catalog behavior and keeps
/// the default crate entirely offline.
pub fn builtin_models(provider: &str) -> Result<Vec<Model>, CatalogLookupError> {
    if !is_selected_text_provider(provider) {
        return Ok(Vec::new());
    }
    #[cfg(feature = "provider-anthropic")]
    if provider == "anthropic" {
        return pi_ai::catalog::get_builtin_models(provider)
            .map_err(|_| CatalogLookupError::InvalidCatalog);
    }
    #[cfg(feature = "provider-deepseek")]
    if provider == "deepseek" {
        return pi_ai::catalog::get_builtin_models(provider)
            .map_err(|_| CatalogLookupError::InvalidCatalog);
    }
    #[cfg(feature = "provider-google")]
    if provider == "google" {
        return pi_ai::catalog::get_builtin_models(provider)
            .map_err(|_| CatalogLookupError::InvalidCatalog);
    }
    #[cfg(feature = "provider-openai")]
    if provider == "openai" {
        return pi_ai::catalog::get_builtin_models(provider)
            .map_err(|_| CatalogLookupError::InvalidCatalog);
    }
    Ok(Vec::new())
}

/// Returns one exact captured text model for a selected provider.
pub fn builtin_model(provider: &str, model_id: &str) -> Result<Option<Model>, CatalogLookupError> {
    Ok(builtin_models(provider)?
        .into_iter()
        .find(|model| model.id.as_str() == model_id))
}
