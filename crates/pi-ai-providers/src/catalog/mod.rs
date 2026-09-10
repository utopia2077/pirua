//! Validated catalogs generated from the dated public input capture.
//!
//! SHA-256 input/source/output provenance is verified offline by the build tooling.
//! Runtime catalog bytes are immutable and feature-selected; decoding still applies
//! the ordinary core model, header policy, endpoint and compatibility invariants.

use pi_ai_core::{
    ImagesModel, ImagesProviderId, Model, ModelHeaderPolicy, ProviderId, RawImagesModel, RawModel,
};
use std::{collections::BTreeSet, error::Error, fmt};

/// Immutable upstream generator revision, not the capture date of its mutable feeds.
pub const SOURCE_COMMIT: &str = "853a80d26c90a14c1886f0ebb8ffaae133ca2185";
/// Fixed generation clock for the dated public capture; historical text values are unknown.
pub const GENERATED_AT: &str = "2026-09-07T06:59:46.000Z";

/// Sanitized reason why an entire catalog was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CatalogErrorKind {
    /// The JSON or model DTO shape is invalid.
    Decode,
    /// A core model/header/endpoint/compatibility invariant failed.
    Validation,
    /// A provider identity differs or a model ID is duplicated.
    Identity,
}

/// Catalog admission failure. Default diagnostics never include input values.
pub struct CatalogError {
    kind: CatalogErrorKind,
    source: Option<Box<dyn Error + Send + Sync>>,
}

impl CatalogError {
    /// Returns the sanitized failure category.
    pub const fn kind(&self) -> CatalogErrorKind {
        self.kind
    }

    fn source(kind: CatalogErrorKind, source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            kind,
            source: Some(Box::new(source)),
        }
    }

    fn identity() -> Self {
        Self {
            kind: CatalogErrorKind::Identity,
            source: None,
        }
    }
}

impl fmt::Debug for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("catalog admission failed")
    }
}

impl Error for CatalogError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

/// Decodes an ordered text-model array atomically under the supplied header policy.
///
/// This validates supplied model data; it does not establish external-data provenance.
pub fn decode_text_catalog(
    bytes: &[u8],
    provider: &ProviderId,
    policy: &ModelHeaderPolicy,
) -> Result<Vec<Model>, CatalogError> {
    let raw: Vec<RawModel> = serde_json::from_slice(bytes)
        .map_err(|error| CatalogError::source(CatalogErrorKind::Decode, error))?;
    let mut ids = BTreeSet::new();
    raw.into_iter()
        .map(|model| {
            if &model.provider != provider || !ids.insert(model.id.clone()) {
                return Err(CatalogError::identity());
            }
            model
                .into_model(policy)
                .map_err(|error| CatalogError::source(CatalogErrorKind::Validation, error))
        })
        .collect()
}

/// Decodes an ordered image-model array atomically under the supplied header policy.
///
/// Image provider IDs occupy their own namespace. Input data gains no provenance claim.
pub fn decode_image_catalog(
    bytes: &[u8],
    provider: &ImagesProviderId,
    policy: &ModelHeaderPolicy,
) -> Result<Vec<ImagesModel>, CatalogError> {
    let raw: Vec<RawImagesModel> = serde_json::from_slice(bytes)
        .map_err(|error| CatalogError::source(CatalogErrorKind::Decode, error))?;
    let mut ids = BTreeSet::new();
    raw.into_iter()
        .map(|model| {
            if &model.provider != provider || !ids.insert(model.id.clone()) {
                return Err(CatalogError::identity());
            }
            model
                .into_model(policy)
                .map_err(|error| CatalogError::source(CatalogErrorKind::Validation, error))
        })
        .collect()
}

macro_rules! text_catalog {
    ($feature:literal, $function:ident, $provider:literal, $file:literal) => {
        #[cfg(feature = $feature)]
        #[doc = concat!("Returns the validated ", $provider, " catalog from the dated capture, in source order.")]
        pub fn $function() -> Result<Vec<Model>, CatalogError> {
            let provider = ProviderId::new($provider)
                .map_err(|error| CatalogError::source(CatalogErrorKind::Identity, error))?;
            decode_text_catalog(include_bytes!($file), &provider, &ModelHeaderPolicy::default())
        }
    };
}

text_catalog!(
    "catalog-openai",
    openai_models,
    "openai",
    "../../../../conformance/catalog/generated/text/openai.json"
);
text_catalog!(
    "catalog-google",
    google_models,
    "google",
    "../../../../conformance/catalog/generated/text/google.json"
);
text_catalog!(
    "catalog-anthropic",
    anthropic_models,
    "anthropic",
    "../../../../conformance/catalog/generated/text/anthropic.json"
);
text_catalog!(
    "catalog-deepseek",
    deepseek_models,
    "deepseek",
    "../../../../conformance/catalog/generated/text/deepseek.json"
);
text_catalog!(
    "catalog-openai-codex",
    openai_codex_models,
    "openai-codex",
    "../../../../conformance/catalog/generated/text/openai-codex.json"
);
text_catalog!(
    "catalog-openrouter",
    openrouter_models,
    "openrouter",
    "../../../../conformance/catalog/generated/text/openrouter.json"
);

/// Returns captured OpenRouter image models; the historical pinned projection is evidence only.
#[cfg(feature = "catalog-openrouter-images")]
pub fn openrouter_image_models() -> Result<Vec<ImagesModel>, CatalogError> {
    let provider = ImagesProviderId::new("openrouter")
        .map_err(|error| CatalogError::source(CatalogErrorKind::Identity, error))?;
    decode_image_catalog(
        include_bytes!("../../../../conformance/catalog/generated/images/openrouter.json"),
        &provider,
        &ModelHeaderPolicy::default(),
    )
}
