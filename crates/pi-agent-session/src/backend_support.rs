//! Narrow pure helpers and host-type names for backend consumers. No catalog authority.
pub use crate::memory::StorageClock;
pub use crate::repo::forks::{lane_writes as lane_seed_writes, prepare as prepare_fork};
pub use crate::storage::values::{
    materialize as materialize_entry, register_key, register_seq,
    set_register as materialize_register, structure as entry_structure,
};
pub use pi_telemetry::{NOOP_TELEMETRY_CONTEXT, NoopTelemetryContext, TelemetryContext};
