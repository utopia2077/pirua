//! Coding transcript trees and context projection with SQLite and Memory stores.
//! This is ordinary saved conversation state, independent of a Durable task runtime.

mod manager;
mod observation;
pub use observation::{HistoryChange, HistorySubscription};
mod projection;
mod store;
mod types;

pub use manager::{CompactionOptions, SessionManager};
pub use projection::{build_context_entries, project, project_entry};
pub use store::{MemoryStore, SessionStore, SqliteStore};
pub use types::*;

use std::fmt;

#[derive(Debug)]
pub enum Error {
    Database(pi_sqlite::Error),
    Json(serde_json::Error),
    NotFound(String),
    AlreadyExists(String),
    Conflict { expected: u64, actual: u64 },
    Invalid(String),
    Entropy,
    Poisoned,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => error.fmt(f),
            Self::Json(error) => error.fmt(f),
            Self::NotFound(id) => write!(f, "session or entry not found: {id}"),
            Self::AlreadyExists(id) => write!(f, "session or entry already exists: {id}"),
            Self::Conflict { expected, actual } => write!(
                f,
                "session revision conflict: expected {expected}, found {actual}"
            ),
            Self::Invalid(message) => f.write_str(message),
            Self::Entropy => f.write_str("failed to generate session identity"),
            Self::Poisoned => f.write_str("session store lock is poisoned"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl From<pi_sqlite::Error> for Error {
    fn from(error: pi_sqlite::Error) -> Self {
        Self::Database(error)
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

fn sqlite(error: impl Into<pi_sqlite::Error>) -> Error {
    Error::Database(error.into())
}
