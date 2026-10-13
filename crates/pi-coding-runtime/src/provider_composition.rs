//! Deterministic provider/model composition over existing `pi-ai` contracts.
//!
//! This layer only admits injected providers, detached models, and host-owned
//! credential capabilities. It does not construct provider transports, refresh
//! catalogs, resolve auth, or publish durable state.

use crate::credentials::CredentialInputs;
use crate::models::{
    CatalogLookupError, DEFAULT_MODEL_PER_PROVIDER, builtin_models, is_selected_text_provider,
    selected_text_provider_ids,
};
use pi_ai::{Model, ModelId, ModelThinkingLevel, Provider, ProviderId};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

// `ModelRef` is owned by the accepted M1a settings snapshot. This import alias
// keeps the facade's public signatures visibly tied to that existing type.
use crate::ModelRef;

/// Stable model/provider composition failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodingRuntimeError {
    /// A required pre-start capability was not supplied.
    MissingCapability { capability: &'static str },
    /// A provider identity was not registered or selected.
    MissingProvider { provider: ProviderId },
    /// A provider was selected but the exact model identity was absent.
    MissingModel {
        provider: ProviderId,
        model_id: ModelId,
    },
    /// A provider/model input is outside the selected product scope.
    UnsupportedProvider { provider: ProviderId },
    /// A model reference did not contain a non-empty provider and model ID.
    InvalidModelReference { reference: String },
    /// Two injected providers used the same identity.
    DuplicateProvider { provider: ProviderId },
    /// Two injected models used the same provider-scoped identity.
    DuplicateModel {
        provider: ProviderId,
        model_id: ModelId,
    },
    /// A selected built-in catalog could not be admitted.
    Catalog(CatalogLookupError),
}

impl fmt::Display for CodingRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingCapability { capability } => {
                write!(formatter, "missing capability: {capability}")
            }
            Self::MissingProvider { .. } => formatter.write_str("missing provider"),
            Self::MissingModel { .. } => formatter.write_str("missing model"),
            Self::UnsupportedProvider { .. } => formatter.write_str("unsupported provider"),
            Self::InvalidModelReference { .. } => formatter.write_str("invalid model reference"),
            Self::DuplicateProvider { .. } => formatter.write_str("duplicate provider"),
            Self::DuplicateModel { .. } => formatter.write_str("duplicate model"),
            Self::Catalog(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for CodingRuntimeError {}

impl From<CatalogLookupError> for CodingRuntimeError {
    fn from(error: CatalogLookupError) -> Self {
        Self::Catalog(error)
    }
}

/// Inputs supplied by a host at the coding-runtime composition boundary.
///
/// All fields are detached references or values owned by the caller. The
/// resulting facade stores the injected handles without taking ownership of
/// transport, network, credential persistence, or durable session state.
pub struct ProviderCompositionInputs {
    /// Provider contracts supplied by the host.
    pub providers: Vec<Arc<dyn Provider>>,
    /// Detached model values, including host overlays or captured catalogs.
    pub models: Vec<Model>,
    /// Host-owned credential capabilities.
    pub credentials: CredentialInputs,
}

impl Default for ProviderCompositionInputs {
    fn default() -> Self {
        Self {
            providers: Vec::new(),
            models: Vec::new(),
            credentials: CredentialInputs::empty(),
        }
    }
}

impl fmt::Debug for ProviderCompositionInputs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderCompositionInputs")
            .field("provider_count", &self.providers.len())
            .field("model_count", &self.models.len())
            .field("credentials", &self.credentials)
            .finish()
    }
}

/// Deterministic, read-only provider/model composition facade.
pub struct ProviderComposition {
    providers: BTreeMap<ProviderId, Arc<dyn Provider>>,
    models: BTreeMap<(ProviderId, ModelId), Model>,
    known_providers: BTreeSet<ProviderId>,
    credentials: CredentialInputs,
    aliases: BTreeMap<String, (ProviderId, ModelId)>,
    availability: BTreeMap<(ProviderId, ModelId), bool>,
}

impl fmt::Debug for ProviderComposition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderComposition")
            .field("provider_count", &self.providers.len())
            .field("model_count", &self.models.len())
            .field("alias_count", &self.aliases.len())
            .field("availability_count", &self.availability.len())
            .field("credential_inputs", &self.credentials)
            .finish()
    }
}

impl ProviderComposition {
    /// Creates a facade from injected providers, detached models, and ports.
    pub fn new(inputs: ProviderCompositionInputs) -> Result<Self, CodingRuntimeError> {
        let mut providers = BTreeMap::new();
        let mut known_providers = BTreeSet::new();

        for provider in inputs.providers {
            let id = provider.id().clone();
            if !is_selected_text_provider(id.as_str()) {
                return Err(CodingRuntimeError::UnsupportedProvider { provider: id });
            }
            if providers.insert(id.clone(), provider).is_some() {
                return Err(CodingRuntimeError::DuplicateProvider { provider: id });
            }
            known_providers.insert(id);
        }

        let mut models = BTreeMap::new();
        for model in inputs.models {
            let provider = model.provider.clone();
            let model_id = model.id.clone();
            if !is_selected_text_provider(provider.as_str()) {
                return Err(CodingRuntimeError::UnsupportedProvider { provider });
            }
            known_providers.insert(model.provider.clone());
            if models
                .insert((model.provider.clone(), model.id.clone()), model)
                .is_some()
            {
                return Err(CodingRuntimeError::DuplicateModel { provider, model_id });
            }
        }

        Ok(Self {
            providers,
            models,
            known_providers,
            credentials: inputs.credentials,
            aliases: BTreeMap::new(),
            availability: BTreeMap::new(),
        })
    }

    /// Adds detached configured models as a higher-precedence runtime overlay.
    ///
    /// The overlay is process-local and never writes the host model store.
    /// Existing provider/model identities are replaced deterministically in
    /// input order, so later detached configuration wins.
    pub fn with_model_overlays(
        mut self,
        overlays: impl IntoIterator<Item = ConfiguredModelOverlay>,
    ) -> Result<Self, CodingRuntimeError> {
        for overlay in overlays {
            let provider = overlay.model.provider.clone();
            let model_id = overlay.model.id.clone();
            if !is_selected_text_provider(provider.as_str()) {
                return Err(CodingRuntimeError::UnsupportedProvider { provider });
            }
            self.known_providers.insert(provider.clone());
            self.models
                .insert((provider.clone(), model_id.clone()), overlay.model);
            let canonical = format!("{provider}:{model_id}");
            self.aliases
                .insert(canonical, (provider.clone(), model_id.clone()));
            for alias in overlay.aliases {
                let normalized = normalize_alias(&alias);
                if normalized.is_empty() {
                    continue;
                }
                self.aliases
                    .insert(normalized, (provider.clone(), model_id.clone()));
            }
        }
        Ok(self)
    }

    /// Applies a host-supplied availability snapshot to this detached facade.
    ///
    /// Availability is an input snapshot; checking it never refreshes remote
    /// catalogs or consults credentials.
    pub fn with_availability(
        mut self,
        availability: impl IntoIterator<Item = ModelAvailability>,
    ) -> Self {
        self.availability = availability
            .into_iter()
            .map(|entry| ((entry.provider, entry.model_id), entry.available))
            .collect();
        self
    }

    /// Creates a facade from the selected, feature-gated captured catalogs.
    pub fn from_builtin_catalogs(
        credentials: CredentialInputs,
    ) -> Result<Self, CodingRuntimeError> {
        let mut models = Vec::new();
        for provider in selected_text_provider_ids() {
            models.extend(builtin_models(provider)?);
        }
        Self::new(ProviderCompositionInputs {
            providers: Vec::new(),
            models,
            credentials,
        })
    }

    /// Creates a selected built-in catalog and applies detached overlays.
    pub fn from_builtin_catalogs_with_overlays(
        credentials: CredentialInputs,
        overlays: impl IntoIterator<Item = ConfiguredModelOverlay>,
        availability: impl IntoIterator<Item = ModelAvailability>,
    ) -> Result<Self, CodingRuntimeError> {
        Self::from_builtin_catalogs(credentials)?
            .with_model_overlays(overlays)
            .map(|composition| composition.with_availability(availability))
    }

    /// Returns injected providers in deterministic provider-ID order.
    pub fn providers(&self) -> impl Iterator<Item = &Arc<dyn Provider>> {
        self.providers.values()
    }

    /// Returns the injected provider for an exact identity.
    pub fn provider(&self, provider: &ProviderId) -> Option<&Arc<dyn Provider>> {
        self.providers.get(provider)
    }

    /// Returns all detached models in deterministic provider/model order.
    pub fn models(&self) -> impl Iterator<Item = &Model> {
        self.models.values()
    }

    /// Selects a coding product default from detached models and availability.
    ///
    /// Preferred IDs follow the pinned selected-provider priority. If none is
    /// usable, the first usable model in deterministic provider/model order is
    /// returned. Explicitly unavailable entries are excluded; absent snapshot
    /// entries remain usable, as in `resolve`. No usable model returns `None`.
    ///
    /// This read-only helper never consults providers or credentials, refreshes
    /// catalogs, or changes a host's explicit model or thinking selection.
    pub fn select_default_model(&self) -> Option<&Model> {
        let available_models = || {
            self.models
                .iter()
                .filter(|(key, _)| self.availability.get(*key) != Some(&false))
                .map(|(_, model)| model)
        };
        for (provider, model_id) in DEFAULT_MODEL_PER_PROVIDER {
            if let Some(model) = available_models()
                .find(|model| model.provider.as_str() == provider && model.id.as_str() == model_id)
            {
                return Some(model);
            }
        }
        available_models().next()
    }

    /// Returns host-owned credential capabilities without invoking them.
    pub fn credentials(&self) -> &CredentialInputs {
        &self.credentials
    }

    /// Resolves the selected model from a detached coding settings snapshot.
    pub fn resolve_settings(
        &self,
        settings: &crate::CodingSettingsSnapshot,
    ) -> Result<&Model, CodingRuntimeError> {
        self.resolve(&settings.model)
    }

    /// Resolves an exact provider-scoped model reference before any request.
    pub fn resolve(&self, model: &ModelRef) -> Result<&Model, CodingRuntimeError> {
        if !self.known_providers.contains(&model.provider) {
            return Err(CodingRuntimeError::MissingProvider {
                provider: model.provider.clone(),
            });
        }
        let key = (model.provider.clone(), model.model_id.clone());
        if self.availability.get(&key) == Some(&false) {
            return Err(CodingRuntimeError::MissingModel {
                provider: model.provider.clone(),
                model_id: model.model_id.clone(),
            });
        }
        self.models
            .get(&key)
            .ok_or_else(|| CodingRuntimeError::MissingModel {
                provider: model.provider.clone(),
                model_id: model.model_id.clone(),
            })
    }

    /// Resolves an exact `provider:model` string before any request.
    ///
    /// Splitting at the first colon preserves all subsequent colons in an
    /// OpenRouter model ID, so `openrouter:vendor/model:variant` remains one
    /// provider-scoped model identity.
    pub fn resolve_reference(&self, reference: &str) -> Result<&Model, CodingRuntimeError> {
        let Some((provider, model_id)) = reference.split_once(':') else {
            return Err(CodingRuntimeError::InvalidModelReference {
                reference: reference.to_owned(),
            });
        };
        if provider.is_empty() || model_id.is_empty() {
            return Err(CodingRuntimeError::InvalidModelReference {
                reference: reference.to_owned(),
            });
        }
        let provider_id =
            ProviderId::new(provider).map_err(|_| CodingRuntimeError::InvalidModelReference {
                reference: reference.to_owned(),
            })?;
        let model_id =
            ModelId::new(model_id).map_err(|_| CodingRuntimeError::InvalidModelReference {
                reference: reference.to_owned(),
            })?;
        self.resolve(&ModelRef::new(provider_id, model_id))
    }

    /// Resolves an exact reference, configured alias, or unqualified fuzzy
    /// alias and selects an optional `@thinking-level` suffix.
    pub fn resolve_selection(
        &self,
        reference: &str,
    ) -> Result<ResolvedModel<'_>, CodingRuntimeError> {
        let (base, requested_level) = split_thinking_suffix(reference)?;
        let model = if base.contains(':') {
            self.resolve_reference(base)?
        } else {
            let alias = normalize_alias(base);
            if let Some((provider, model_id)) = self.aliases.get(&alias) {
                self.resolve(&ModelRef::new(provider.clone(), model_id.clone()))?
            } else {
                let mut matches = self.models.iter().filter(|((_, model_id), model)| {
                    normalize_alias(model_id.as_str()).contains(&alias)
                        || normalize_alias(&model.name).contains(&alias)
                });
                let Some(((provider, model_id), _)) = matches.next() else {
                    return Err(CodingRuntimeError::MissingModel {
                        provider: ProviderId::new("unknown").expect("static provider"),
                        model_id: ModelId::new(base).map_err(|_| {
                            CodingRuntimeError::InvalidModelReference {
                                reference: reference.to_owned(),
                            }
                        })?,
                    });
                };
                if matches.next().is_some() {
                    return Err(CodingRuntimeError::InvalidModelReference {
                        reference: reference.to_owned(),
                    });
                }
                self.resolve(&ModelRef::new(provider.clone(), model_id.clone()))?
            }
        };
        let thinking_level = requested_level.unwrap_or(ModelThinkingLevel::Off);
        if thinking_level != ModelThinkingLevel::Off && !model.reasoning {
            return Err(CodingRuntimeError::InvalidModelReference {
                reference: reference.to_owned(),
            });
        }
        if model
            .thinking_level_map
            .as_ref()
            .and_then(|mapping| mapping.as_map().get(&thinking_level))
            == Some(&None)
        {
            return Err(CodingRuntimeError::InvalidModelReference {
                reference: reference.to_owned(),
            });
        }
        Ok(ResolvedModel {
            model,
            thinking_level,
        })
    }

    /// Returns whether a required composition capability is available.
    pub fn has_capability(&self, capability: &str) -> bool {
        match capability {
            "models.resolve" => !self.models.is_empty(),
            "providers.resolve" => !self.known_providers.is_empty(),
            _ => false,
        }
    }

    /// Requires a named capability before operation admission.
    pub fn require_capability(&self, capability: &'static str) -> Result<(), CodingRuntimeError> {
        if self.has_capability(capability) {
            Ok(())
        } else {
            Err(CodingRuntimeError::MissingCapability { capability })
        }
    }
}

/// One detached model overlay supplied by the host or a deterministic test
/// adapter. Overlays are never persisted by the coding runtime.
#[derive(Clone, Debug)]
pub struct ConfiguredModelOverlay {
    /// Model value to overlay.
    pub model: Model,
    /// Exact or fuzzy aliases accepted during selection.
    pub aliases: Vec<String>,
}

impl ConfiguredModelOverlay {
    /// Creates an overlay with no aliases.
    pub fn new(model: Model) -> Self {
        Self {
            model,
            aliases: Vec::new(),
        }
    }

    /// Adds one detached alias.
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.aliases.push(alias.into());
        self
    }
}

/// One host-supplied model availability observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelAvailability {
    /// Provider identity.
    pub provider: ProviderId,
    /// Model identity.
    pub model_id: ModelId,
    /// Whether the snapshot marks the model usable.
    pub available: bool,
}

/// A selected model and its resolved public thinking level.
#[derive(Clone, Copy, Debug)]
pub struct ResolvedModel<'a> {
    /// Selected model.
    pub model: &'a Model,
    /// Selected thinking level.
    pub thinking_level: ModelThinkingLevel,
}

fn normalize_alias(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace(['_', '-', ' '], "")
}

fn split_thinking_suffix(
    reference: &str,
) -> Result<(&str, Option<ModelThinkingLevel>), CodingRuntimeError> {
    let Some((base, suffix)) = reference.rsplit_once('@') else {
        return Ok((reference, None));
    };
    let level = match suffix.to_ascii_lowercase().as_str() {
        "off" => ModelThinkingLevel::Off,
        "minimal" => ModelThinkingLevel::Minimal,
        "low" => ModelThinkingLevel::Low,
        "medium" => ModelThinkingLevel::Medium,
        "high" => ModelThinkingLevel::High,
        "xhigh" => ModelThinkingLevel::ExtraHigh,
        "max" => ModelThinkingLevel::Max,
        _ => {
            return Err(CodingRuntimeError::InvalidModelReference {
                reference: reference.to_owned(),
            });
        }
    };
    if base.is_empty() {
        return Err(CodingRuntimeError::InvalidModelReference {
            reference: reference.to_owned(),
        });
    }
    Ok((base, Some(level)))
}
