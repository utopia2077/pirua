//! Local JavaScript tool orchestration in a dedicated, killable Rust helper
//! process. Only declared JSON tool/global calls cross the VM capability boundary.
//! See the crate README for helper packaging, limits and the replay contract.
mod host;
mod json;
mod protocol;
mod types;
mod worker;

pub use host::{Run, Sandbox};
pub use types::*;
#[doc(hidden)]
pub use worker::worker_main;
