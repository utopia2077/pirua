//! Concrete Harness runtime components. Full driver and Effects composition are separate.

#[cfg(test)]
extern crate self as pi_harness_runtime;

#[cfg(all(test, feature = "extensions"))]
#[path = "../tests/runtime_extension_profiles.rs"]
pub(crate) mod runtime_extension_profiles;

#[cfg(feature = "extensions")]
pub mod extensions;
pub mod hooks;
pub mod manual_gate;
pub mod scheduler;

pub mod restore;

pub mod owner;

mod events;

mod publication;

mod lane;
mod tree;

mod snapshots;

mod harness;

mod effects;

mod driver;

mod tools;

pub mod provider;

pub mod request;

mod resources;

mod telemetry;

mod abort;
mod checkpoint;
mod inbox;
mod poll;

/// Pure projected structural preparation and summary content helpers.
pub mod summary_content;

mod compaction;
mod preparation;
mod structural;
mod summary;
