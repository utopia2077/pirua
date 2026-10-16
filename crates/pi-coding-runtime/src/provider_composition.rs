//! Deterministic provider/model composition over existing `pi-ai` contracts.
//!
//! This layer only admits injected providers, detached models, and host-owned
//! credential capabilities. It does not construct provider transports, refresh
//! catalogs, resolve auth, or publish durable state.

use crate::credentials::CredentialInputs;
use crate::models::{
    CatalogLookupError, DEFAULT_MODEL_PER_PROVIDER, builtin_models, selected_text_provider_ids,
};
use pi_ai::{Model, ModelId, ModelThinkingLevel, Provider, ProviderId};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

// Model references are detached coding configuration, independent of runtime state.
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
    model_order: Vec<(ProviderId, ModelId)>,
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
            if providers.insert(id.clone(), provider).is_some() {
                return Err(CodingRuntimeError::DuplicateProvider { provider: id });
            }
            known_providers.insert(id);
        }

        let mut models = BTreeMap::new();
        let mut model_order = Vec::new();
        for model in inputs.models {
            let provider = model.provider.clone();
            let model_id = model.id.clone();
            known_providers.insert(model.provider.clone());
            model_order.push((provider.clone(), model_id.clone()));
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
            model_order,
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
            self.known_providers.insert(provider.clone());
            let key = (provider.clone(), model_id.clone());
            if !self.models.contains_key(&key) {
                self.model_order.push(key.clone());
            }
            self.models.insert(key, overlay.model);
            let canonical = format!("{provider}/{model_id}");
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

    /// Returns detached models in their host/catalog admission order.
    pub fn models(&self) -> impl Iterator<Item = &Model> {
        self.model_order
            .iter()
            .filter_map(|key| self.models.get(key))
    }

    /// Selects a coding product default from detached models and availability.
    ///
    /// Preferred IDs follow the pinned selected-provider priority. If none is
    /// usable, the first usable model in host/catalog admission order is
    /// returned. Explicitly unavailable entries are excluded; absent snapshot
    /// entries remain usable, as in `resolve`. No usable model returns `None`.
    ///
    /// This read-only helper never consults providers or credentials, refreshes
    /// catalogs, or changes a host's explicit model or thinking selection.
    pub fn select_default_model(&self) -> Option<&Model> {
        let available_models = || {
            self.models().filter(|model| {
                self.availability
                    .get(&(model.provider.clone(), model.id.clone()))
                    != Some(&false)
            })
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

    /// Resolve a release-style provider/id, exact ID, configured alias or fuzzy reference.
    pub fn resolve_selection(
        &self,
        reference: &str,
    ) -> Result<ResolvedModel<'_>, CodingRuntimeError> {
        if let Some((provider, id)) = self.aliases.get(&normalize_alias(reference)) {
            return Ok(ResolvedModel {
                model: self.resolve(&ModelRef::new(provider.clone(), id.clone()))?,
                thinking_level: ModelThinkingLevel::Off,
            });
        }
        let pool: Vec<_> = self.models().cloned().collect();
        let request = ModelReferenceRequest {
            text: reference,
            provider: None,
            thinking_suffix: ThinkingSuffix::Strict,
            fuzzy: true,
        };
        match resolve_model_reference(&request, &pool, |provider| {
            self.models
                .iter()
                .any(|(key, _)| &key.0 == provider && self.availability.get(key) != Some(&false))
        }) {
            ModelReferenceResult::Resolved {
                model,
                thinking_level,
                ..
            } => Ok(ResolvedModel {
                model: self.resolve(&ModelRef::new(model.provider.clone(), model.id.clone()))?,
                thinking_level: thinking_level.unwrap_or(ModelThinkingLevel::Off),
            }),
            ModelReferenceResult::Ambiguous { .. } => {
                Err(CodingRuntimeError::InvalidModelReference {
                    reference: reference.into(),
                })
            }
            ModelReferenceResult::NotFound { .. } => Err(CodingRuntimeError::MissingModel {
                provider: ProviderId::new("unknown").expect("static provider"),
                model_id: ModelId::new(reference).map_err(|_| {
                    CodingRuntimeError::InvalidModelReference {
                        reference: reference.into(),
                    }
                })?,
            }),
        }
    }

    /// Exact identity resolution without fuzzy matching or thinking suffix parsing.
    pub fn resolve_reference(&self, reference: &str) -> Result<&Model, CodingRuntimeError> {
        let pool: Vec<_> = self.models().cloned().collect();
        match resolve_model_reference(
            &ModelReferenceRequest {
                text: reference,
                provider: None,
                thinking_suffix: ThinkingSuffix::None,
                fuzzy: false,
            },
            &pool,
            |_| true,
        ) {
            ModelReferenceResult::Resolved { model, .. } => {
                self.resolve(&ModelRef::new(model.provider.clone(), model.id.clone()))
            }
            _ => Err(CodingRuntimeError::InvalidModelReference {
                reference: reference.into(),
            }),
        }
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
    value.trim().to_lowercase()
}

/// Whether trailing colon suffixes are interpreted as thinking preferences.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThinkingSuffix {
    None,
    Strict,
    Lenient,
}
/// Detached model lookup policy, matching the release's model-resolver.
#[derive(Clone, Debug)]
pub struct ModelReferenceRequest<'a> {
    pub text: &'a str,
    pub provider: Option<&'a str>,
    pub thinking_suffix: ThinkingSuffix,
    pub fuzzy: bool,
}
#[derive(Clone, Debug)]
pub struct ModelReferenceTarget<'a> {
    pub provider: &'a ProviderId,
    pub provider_models: Vec<&'a Model>,
    pub model_text: String,
    pub model_id: String,
    pub thinking_level: Option<ModelThinkingLevel>,
}
#[derive(Clone, Debug)]
pub enum ModelReferenceResult<'a> {
    Resolved {
        model: &'a Model,
        thinking_level: Option<ModelThinkingLevel>,
        warning: Option<String>,
    },
    Ambiguous {
        candidates: Vec<&'a Model>,
        usable_count: usize,
        thinking_level: Option<ModelThinkingLevel>,
        warning: Option<String>,
    },
    NotFound {
        target: Option<ModelReferenceTarget<'a>>,
    },
}

/// Resolve literal IDs before suffixes; provider usability only breaks cross-provider ties.
pub fn resolve_model_reference<'a>(
    request: &ModelReferenceRequest<'_>,
    models: &'a [Model],
    usable: impl Fn(&ProviderId) -> bool,
) -> ModelReferenceResult<'a> {
    let text = request.text.trim();
    let pool: Vec<_> = models
        .iter()
        .filter(|model| {
            request
                .provider
                .is_none_or(|provider| model.provider.as_str().eq_ignore_ascii_case(provider))
        })
        .collect();
    if text.is_empty() {
        return ModelReferenceResult::NotFound { target: None };
    }
    if let Some(result) = resolve_text(text, request, &pool, &usable) {
        return result;
    }
    let split = provider_prefix(text, &pool);
    let anchor = if request.provider.is_some() {
        pool.first().copied()
    } else {
        split.and_then(|(provider, _)| {
            pool.iter()
                .copied()
                .find(|model| model.provider.as_str() == provider)
        })
    };
    let target = anchor.map(|anchor| {
        let model_text = split.map_or(text, |(_, id)| id);
        let suffix = (request.thinking_suffix != ThinkingSuffix::None)
            .then(|| thinking_suffix(model_text))
            .flatten();
        ModelReferenceTarget {
            provider: &anchor.provider,
            provider_models: pool
                .iter()
                .copied()
                .filter(|model| model.provider == anchor.provider)
                .collect(),
            model_text: model_text.into(),
            model_id: suffix.map_or(model_text, |(base, _)| base).into(),
            thinking_level: suffix.map(|(_, level)| level),
        }
    });
    ModelReferenceResult::NotFound { target }
}
fn thinking_suffix(text: &str) -> Option<(&str, ModelThinkingLevel)> {
    let (base, suffix) = text.rsplit_once(':')?;
    let level = match suffix {
        "off" => ModelThinkingLevel::Off,
        "minimal" => ModelThinkingLevel::Minimal,
        "low" => ModelThinkingLevel::Low,
        "medium" => ModelThinkingLevel::Medium,
        "high" => ModelThinkingLevel::High,
        "xhigh" => ModelThinkingLevel::ExtraHigh,
        "max" => ModelThinkingLevel::Max,
        _ => return None,
    };
    Some((base, level))
}
fn provider_prefix<'a, 'b>(text: &'b str, pool: &[&'a Model]) -> Option<(&'a str, &'b str)> {
    let (provider, model_text) = text.split_once('/')?;
    let provider = provider.trim();
    let model_text = model_text.trim();
    if provider.is_empty() || model_text.is_empty() {
        return None;
    }
    pool.iter()
        .find(|model| model.provider.as_str().eq_ignore_ascii_case(provider))
        .map(|model| (model.provider.as_str(), model_text))
}
fn resolved(model: &Model) -> ModelReferenceResult<'_> {
    ModelReferenceResult::Resolved {
        model,
        thinking_level: None,
        warning: None,
    }
}
fn best_fuzzy<'a>(models: &[&'a Model]) -> Option<&'a Model> {
    let is_alias = |model: &&Model| {
        let id = model.id.as_str();
        id.ends_with("-latest")
            || !id.rsplit_once('-').is_some_and(|(_, suffix)| {
                suffix.len() == 8 && suffix.bytes().all(|ch| ch.is_ascii_digit())
            })
    };
    let aliases: Vec<_> = models.iter().copied().filter(is_alias).collect();
    let pool = if aliases.is_empty() { models } else { &aliases };
    pool.iter()
        .copied()
        .max_by(|a, b| a.id.as_str().cmp(b.id.as_str()))
}
fn fuzzy_matches<'a>(text: &str, pool: &[&'a Model]) -> Vec<&'a Model> {
    let text = text.to_lowercase();
    pool.iter()
        .copied()
        .filter(|model| {
            model.id.as_str().to_lowercase().contains(&text)
                || model.name.to_lowercase().contains(&text)
        })
        .collect()
}
fn select_reference<'a>(
    text: &str,
    pool: &[&'a Model],
    fuzzy: bool,
    usable: &impl Fn(&ProviderId) -> bool,
) -> Option<ModelReferenceResult<'a>> {
    let raw: Vec<_> = pool
        .iter()
        .copied()
        .filter(|model| model.id.as_str().eq_ignore_ascii_case(text))
        .collect();
    let named = provider_prefix(text, pool).and_then(|(provider, model_text)| {
        let own: Vec<_> = pool
            .iter()
            .copied()
            .filter(|model| model.provider.as_str() == provider)
            .collect();
        own.iter()
            .copied()
            .find(|model| model.id.as_str().eq_ignore_ascii_case(model_text))
            .or_else(|| {
                raw.iter()
                    .copied()
                    .find(|model| model.provider.as_str() == provider)
            })
            .or_else(|| {
                fuzzy
                    .then(|| best_fuzzy(&fuzzy_matches(model_text, &own)))
                    .flatten()
            })
    });
    let others: Vec<_> = raw
        .iter()
        .copied()
        .filter(|model| named.is_none_or(|named| !std::ptr::eq(*model, named)))
        .collect();
    let candidates: Vec<_> = named.into_iter().chain(others.iter().copied()).collect();
    let first = *candidates.first()?;
    if candidates
        .iter()
        .all(|model| model.provider == first.provider)
    {
        return Some(resolved(first));
    }
    if let Some(named) = named.filter(|model| usable(&model.provider)) {
        return Some(resolved(named));
    }
    let usable_others: Vec<_> = others
        .iter()
        .copied()
        .filter(|model| usable(&model.provider))
        .collect();
    if usable_others.len() == 1 {
        return Some(resolved(usable_others[0]));
    }
    if usable_others.is_empty()
        && let Some(named) = named
    {
        return Some(resolved(named));
    }
    Some(ModelReferenceResult::Ambiguous {
        candidates,
        usable_count: usable_others.len(),
        thinking_level: None,
        warning: None,
    })
}
fn select_fuzzy<'a>(
    matches: &[&'a Model],
    usable: &impl Fn(&ProviderId) -> bool,
) -> Option<ModelReferenceResult<'a>> {
    let multiple = matches
        .first()
        .is_some_and(|first| matches.iter().any(|model| model.provider != first.provider));
    let available: Vec<_> = matches
        .iter()
        .copied()
        .filter(|model| multiple && usable(&model.provider))
        .collect();
    let pool = if available.is_empty() {
        matches
    } else {
        &available
    };
    let best = best_fuzzy(pool)?;
    let tied: Vec<_> = pool
        .iter()
        .copied()
        .filter(|model| model.id == best.id)
        .collect();
    if tied.iter().all(|model| model.provider == best.provider) {
        return Some(resolved(best));
    }
    Some(ModelReferenceResult::Ambiguous {
        usable_count: if available.is_empty() { 0 } else { tied.len() },
        candidates: tied,
        thinking_level: None,
        warning: None,
    })
}
fn resolve_text<'a>(
    text: &str,
    request: &ModelReferenceRequest<'_>,
    pool: &[&'a Model],
    usable: &impl Fn(&ProviderId) -> bool,
) -> Option<ModelReferenceResult<'a>> {
    if let Some(result) = select_reference(text, pool, request.fuzzy, usable) {
        return Some(result);
    }
    if request.fuzzy
        && let Some(result) = select_fuzzy(&fuzzy_matches(text, pool), usable)
    {
        return Some(result);
    }
    if request.thinking_suffix == ThinkingSuffix::None {
        return None;
    }
    let (base, level, warning) = if let Some((base, level)) = thinking_suffix(text) {
        (base, Some(level), None)
    } else if request.thinking_suffix == ThinkingSuffix::Lenient {
        let (base, suffix) = text.rsplit_once(':')?;
        (
            base,
            None,
            Some(format!(
                "Invalid thinking level \"{suffix}\" in pattern \"{text}\". Using default instead."
            )),
        )
    } else {
        return None;
    };
    let mut result = resolve_text(base, request, pool, usable)?;
    match &mut result {
        ModelReferenceResult::Resolved {
            thinking_level,
            warning: current,
            ..
        }
        | ModelReferenceResult::Ambiguous {
            thinking_level,
            warning: current,
            ..
        } => {
            *thinking_level = if current.is_some() { None } else { level };
            if warning.is_some() {
                *current = warning;
                *thinking_level = None;
            }
        }
        ModelReferenceResult::NotFound { .. } => unreachable!(),
    }
    Some(result)
}
