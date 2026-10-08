//! Deterministic provider/model composition over existing `pi-ai` contracts.
//!
//! This layer only admits injected providers, detached models, and host-owned
//! credential capabilities. It does not construct provider transports, refresh
//! catalogs, resolve auth, or publish durable state.

use crate::credentials::CredentialInputs;
use crate::models::{
    CatalogLookupError, builtin_models, is_selected_text_provider, selected_text_provider_ids,
};
use pi_ai::{Model, ModelId, Provider, ProviderId};
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
}

impl fmt::Debug for ProviderComposition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderComposition")
            .field("provider_count", &self.providers.len())
            .field("model_count", &self.models.len())
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
        })
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
        self.models
            .get(&(model.provider.clone(), model.model_id.clone()))
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
