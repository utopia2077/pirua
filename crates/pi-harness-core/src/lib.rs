//! Private pure Harness planner crate for the Rust harness workspace.

pub mod current_validation;
pub use current_validation::{
    CurrentReferences, CurrentRoots, CurrentValidationError, EntryConstraint, EntryReference,
    RegisterConstraint, collect_current_references, validate_current_entries_and_registers,
};
