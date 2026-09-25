//! Host-owned extension adapters. Enabled only with the protocol dependency.

pub mod critical_port;
pub mod domain;
// Preparatory storage only; no live route acquires a lease yet.
#[allow(dead_code)]
mod index_pool;
pub mod key;
pub mod tool_driver;
pub mod transport;
pub mod value;
