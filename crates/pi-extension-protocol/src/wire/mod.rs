//! Checked-in core and tool protobuf DTOs for the two supported minor versions.
//!
//! The canonical descriptors in `schema/descriptors` define the wire and method
//! contract. These modules contain only generated message code. Run the isolated
//! `codegen` package to verify them against the descriptors after schema changes.

pub mod v1_0 {
    include!("generated/v1_0.rs");
}

pub mod v1_1 {
    include!("generated/v1_1.rs");
}

pub mod echo_bounded;
pub mod echo_selected;
pub mod framing;
pub mod value;
