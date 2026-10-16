//! Validated selected chat catalogs from the exact Pi v1.4.0 release.
//!
//! SHA-256 input/source/output provenance is verified offline by the build tooling.
//! Runtime catalog bytes are immutable and feature-selected; decoding still applies
//! the ordinary core model, header policy, endpoint and compatibility invariants.

use pi_ai_core::{Model, ModelHeaderPolicy, ProviderId, RawModel};
use std::{collections::BTreeSet, error::Error, fmt};

/// Exact target release source commit.
pub const SOURCE_COMMIT: &str = "4a080252f06caf63a525d7873c5d94d3c23887e0";
/// Published package whose published provenance identifies the target commit.
pub const SOURCE_PACKAGE: &str = "@earendil-works/pi-ai@1.4.0";
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

macro_rules! text_catalog {
    ($feature:literal, $function:ident, $provider:literal, $file:literal) => {
        #[cfg(feature = $feature)]
        #[doc = concat!("Returns the validated ", $provider, " catalog from Pi v1.4.0, in source order.")]
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
