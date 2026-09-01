//! Standalone derived search service contract.

use pi_ai_core::{FiniteF64, SafeUnsigned};
use serde::{Deserialize, Serialize};

use crate::serde_support::deserialize_optional_non_null;
use crate::surface::BoxFuture;
use crate::{EntryId, SessionId};

/// Search query shared by session and entry search.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchQuery {
    /// Search text.
    pub text: String,
    /// Optional method-unit result limit.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub limit: Option<SafeUnsigned>,
}

/// Best matching entry projection for one session hit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionSearchTopEntry {
    /// Entry identity.
    pub entry_id: EntryId,
    /// Optional display snippet.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub snippet: Option<String>,
    /// Entry timestamp in Unix milliseconds.
    pub timestamp: SafeUnsigned,
}

/// Ranked session search hit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionSearchHit {
    /// Session identity.
    pub session_id: SessionId,
    /// Optional backend score.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub score: Option<FiniteF64>,
    /// Optional best matching entry.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub top: Option<SessionSearchTopEntry>,
}

/// Ranked entry search hit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EntrySearchHit {
    /// Session identity.
    pub session_id: SessionId,
    /// Entry identity.
    pub entry_id: EntryId,
    /// Entry timestamp in Unix milliseconds.
    pub timestamp: SafeUnsigned,
    /// Optional display snippet.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub snippet: Option<String>,
    /// Optional backend score.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub score: Option<FiniteF64>,
}

/// Behavior-free standalone search service.
pub trait SessionSearchService: Send + Sync {
    /// Backend-specific non-serialized search failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Searches sessions ranked by best match.
    fn search_sessions(
        &self,
        query: SearchQuery,
    ) -> BoxFuture<'_, Result<Vec<SessionSearchHit>, Self::Error>>;
    /// Optionally searches entries. `None` declares that the capability is absent.
    fn search_entries(
        &self,
        query: SearchQuery,
    ) -> Option<BoxFuture<'_, Result<Vec<EntrySearchHit>, Self::Error>>>;
    /// Enumerates repository sessions and catches up derived cursors.
    fn sync(&self) -> BoxFuture<'_, Result<(), Self::Error>>;
    /// Sends a synchronous freshness hint without content.
    fn notify(&self, session_id: &SessionId);
    /// Removes one session's derived index state.
    fn remove(&self, session_id: SessionId) -> BoxFuture<'_, Result<(), Self::Error>>;
    /// Idempotently closes the service.
    fn close(&self) -> BoxFuture<'_, Result<(), Self::Error>>;
}

/// Exact standalone search method inventory.
pub const SEARCH_METHODS: &[&str] = &[
    "search_sessions",
    "search_entries",
    "sync",
    "notify",
    "remove",
    "close",
];
