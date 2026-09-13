//! Standalone derived session search. The optional reference implementation has
//! no canonical storage or Harness runtime authority.
pub use pi_agent_schema::surface::search::{
    EntrySearchHit, SearchQuery, SessionSearchHit, SessionSearchService, SessionSearchTopEntry,
};

#[cfg(feature = "sqlite-fts")]
pub mod sqlite;
#[cfg(feature = "sqlite-fts")]
pub use sqlite::{
    DefaultTextProjector, SearchError, SearchOptions, SqliteSearchService, TextProjector,
};
