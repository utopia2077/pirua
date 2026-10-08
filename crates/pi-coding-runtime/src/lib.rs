//! Private headless coding-runtime composition crate for the Rust harness.

pub mod settings_snapshot;

pub use settings_snapshot::{
    CodingSettingsSnapshot, CompactionSettings, ModelRef, QueueMode, ThinkingLevel,
};
