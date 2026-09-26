//! Host-owned extension adapters. Enabled only with the protocol dependency.

pub mod critical_port;
pub mod domain;
// Preparatory storage only; no live route acquires a lease yet.
#[allow(dead_code)]
mod index_pool;
pub mod key;
// Paired caller backing only; warmup and whole-receive certification are pending.
#[allow(dead_code)]
mod receive_backing;
pub mod tool_driver;
pub mod transport;
pub mod value;
