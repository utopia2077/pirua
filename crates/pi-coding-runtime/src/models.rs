//! Selected coding-runtime entrances and immutable catalog lookup.
//!
//! The catalog functions are thin wrappers over the captured `pi-ai` catalogs.
//! They do not refresh catalogs, inspect credentials, or access a network.

use pi_ai::{ImagesModel, Model, ModelThinkingLevel};
use std::fmt;

// Selected text defaults, preserving their relative order in the pinned
// pi-mono 853a80d26c90a14c1886f0ebb8ffaae133ca2185 model-resolver.ts table.
pub(crate) const DEFAULT_MODEL_PER_PROVIDER: [(&str, &str); 6] = [
    ("anthropic", "claude-opus-4-8"),
    ("openai", "gpt-5.5"),
    ("openai-codex", "gpt-5.5"),
    ("deepseek", "deepseek-v4-pro"),
    ("google", "gemini-3.1-pro-preview"),
    ("openrouter", "moonshotai/kimi-k2.6"),
];

/// Returns the coding product's pinned default thinking preference.
///
/// Hosts may use this preference when normalizing their settings. It does not
/// change explicit model selection, validate model thinking support, or replace
/// `resolve_selection`'s Off behavior when no thinking suffix is supplied.
pub const fn default_thinking_level() -> ModelThinkingLevel {
    ModelThinkingLevel::Medium
}

/// Kind of selected coding-runtime entrance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntranceKind {
    /// Text model/provider entrance.
    Text,
    /// Image generation entrance.
    Image,
}

/// Stable metadata for one selected provider or image entrance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntranceMetadata {
    /// Public entrance identity. Image entrances use `openrouter-images`.
    pub id: &'static str,
    /// Provider identity used by the underlying `pi-ai` contract.
    pub provider: &'static str,
    /// Whether this is a text or image entrance.
    pub kind: EntranceKind,
}

const TEXT_ENTRANCES: [EntranceMetadata; 6] = [
    EntranceMetadata {
        id: "openai",
        provider: "openai",
        kind: EntranceKind::Text,
    },
    EntranceMetadata {
        id: "google",
        provider: "google",
        kind: EntranceKind::Text,
    },
    EntranceMetadata {
        id: "anthropic",
        provider: "anthropic",
        kind: EntranceKind::Text,
    },
    EntranceMetadata {
        id: "deepseek",
        provider: "deepseek",
        kind: EntranceKind::Text,
    },
    EntranceMetadata {
        id: "openai-codex",
        provider: "openai-codex",
        kind: EntranceKind::Text,
    },
    EntranceMetadata {
        id: "openrouter",
        provider: "openrouter",
        kind: EntranceKind::Text,
    },
];

const IMAGE_ENTRANCES: [EntranceMetadata; 1] = [EntranceMetadata {
    id: "openrouter-images",
    provider: "openrouter",
    kind: EntranceKind::Image,
}];

/// Returns the six selected text entrances in stable product order.
pub const fn selected_text_entrances() -> &'static [EntranceMetadata] {
    &TEXT_ENTRANCES
}

/// Returns the separate selected image entrance in stable product order.
pub const fn selected_image_entrances() -> &'static [EntranceMetadata] {
    &IMAGE_ENTRANCES
}

/// Returns all selected entrances, with the image entrance last.
pub fn selected_entrances() -> Vec<EntranceMetadata> {
    TEXT_ENTRANCES.into_iter().chain(IMAGE_ENTRANCES).collect()
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

/// Returns whether an identity is the selected image entrance or provider.
pub fn is_selected_image_entrance(entrance: &str) -> bool {
    IMAGE_ENTRANCES
        .iter()
        .any(|entry| entry.id == entrance || entry.provider == entrance)
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
    #[cfg(feature = "provider-openai-codex")]
    if provider == "openai-codex" {
        return pi_ai::catalog::get_builtin_models(provider)
            .map_err(|_| CatalogLookupError::InvalidCatalog);
    }
    #[cfg(feature = "provider-openrouter")]
    if provider == "openrouter" {
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

/// Returns the captured image catalog for the selected image entrance.
///
/// Both `openrouter-images` (the product entrance) and `openrouter` (the
/// underlying image-provider identity) are accepted as aliases.
pub fn builtin_image_models(entrance: &str) -> Result<Vec<ImagesModel>, CatalogLookupError> {
    if !is_selected_image_entrance(entrance) {
        return Ok(Vec::new());
    }
    #[cfg(feature = "images-openrouter")]
    return pi_ai::catalog::get_builtin_images_models("openrouter")
        .map_err(|_| CatalogLookupError::InvalidCatalog);
    #[cfg(not(feature = "images-openrouter"))]
    {
        Ok(Vec::new())
    }
}

/// Returns one exact captured image model for the selected image entrance.
pub fn builtin_image_model(
    entrance: &str,
    model_id: &str,
) -> Result<Option<ImagesModel>, CatalogLookupError> {
    Ok(builtin_image_models(entrance)?
        .into_iter()
        .find(|model| model.id.as_str() == model_id))
}
