//! Shared real tool phases. The caller owns admission, binding currentness,
//! once-per-batch context resolution, replay decisions and finalization order.

mod agent;
pub use agent::*;
mod tools;
pub use tools::*;
mod batch;
pub use batch::{execute_legacy_tool_batch, execute_tool_batch};
