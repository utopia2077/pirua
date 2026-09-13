use pi_agent_schema::{SessionError, StorageError};

/// Content-free pure import failure; backend and codec sources remain typed.
#[derive(Debug)]
pub enum LegacyError {
    /// Invalid JSON, field or unsupported format.
    InvalidData,
    /// Original parent graph has duplicate IDs or unavailable/forward parents.
    Ancestry,
    /// The first-kept bound is not an earlier node on the compaction's own branch.
    CompactionBoundary,
    /// Timestamp falls outside the specified ISO/calendar/48-bit domain.
    Timestamp,
    /// Remint provider returned an invalid format, timestamp or duplicate ID.
    Identity,
    /// Usage or sequence arithmetic exceeded the accepted numeric range.
    Overflow,
    /// Runtime codec rejected a known message or registered role.
    Codec(SessionError),
    /// Complete normalized snapshot failed the accepted Memory invariants.
    Snapshot(StorageError),
}
impl std::fmt::Display for LegacyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidData => "invalid legacy format-3 data",
            Self::Ancestry => "invalid legacy ancestry",
            Self::CompactionBoundary => "invalid legacy compaction boundary",
            Self::Timestamp => "invalid legacy ISO timestamp",
            Self::Identity => "invalid legacy reminted identity",
            Self::Overflow => "legacy normalized numeric range exceeded",
            Self::Codec(_) => "legacy message schema rejected",
            Self::Snapshot(_) => "legacy normalized snapshot rejected",
        })
    }
}
impl std::error::Error for LegacyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            Self::Snapshot(error) => Some(error),
            _ => None,
        }
    }
}
