//! One-session SQLite storage with indexed reads and fenced writer ownership.
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use pi_agent_session::{backend_support::*, codec::SessionCodec, storage::*};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, Weak},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
mod branch;
mod commit;
mod data;
mod owner;
mod path;
pub mod repo;
#[cfg(test)]
mod tests;
pub use owner::SqliteStorage;
/// Explicit SQLite synchronous policy; default FULL.
#[derive(Clone, Copy, Debug, Default)]
pub enum Synchronous {
    #[default]
    Full,
    Normal,
}
/// Native lease/connection settings; no application clock controls lease admission.
#[derive(Clone, Copy, Debug)]
pub struct SqliteOptions {
    pub busy_timeout_ms: SafeUnsigned,
    pub lease_ttl_ms: SafeUnsigned,
    pub heartbeat_ms: SafeUnsigned,
    pub synchronous: Synchronous,
}
impl Default for SqliteOptions {
    fn default() -> Self {
        Self {
            busy_timeout_ms: n(5000),
            lease_ttl_ms: n(30000),
            heartbeat_ms: n(10000),
            synchronous: Synchronous::Full,
        }
    }
}
impl SqliteOptions {
    fn validate(self) -> Result<(), StorageError> {
        if self.busy_timeout_ms.get() == 0
            || self.heartbeat_ms.get() == 0
            || self.heartbeat_ms >= self.lease_ttl_ms
        {
            return Err(invalid("invalid SQLite timing options"));
        }
        Ok(())
    }
}
/// Opaque fresh physical-store identity, distinct from revision and lease fence.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct StoreToken([u8; 32]);
impl StoreToken {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
impl std::fmt::Debug for StoreToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StoreToken([REDACTED])")
    }
}
/// Coherent explicit administrative snapshot, not an ordinary hot-path read.
#[derive(Clone, Debug)]
pub struct SnapshotObservation {
    pub metadata: SessionMetadata,
    pub snapshot: StorageSnapshot,
    pub store_token: StoreToken,
    pub revision: SafeUnsigned,
}
fn n(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).expect("bounded constant")
}
fn context(text: &'static str) -> ErrorContext {
    ErrorContext::new(text).expect("bounded static SQLite context")
}
fn invalid(text: &'static str) -> StorageError {
    StorageError::InvalidData(context(text))
}
fn corrupt(text: &'static str) -> StorageError {
    StorageError::Corruption(context(text))
}
fn busy() -> StorageError {
    StorageError::Busy(context("SQLite ownership is busy"))
}
fn closed() -> StorageError {
    StorageError::Closed(context("SQLite owner is closed or stale"))
}
fn io(_: std::io::Error) -> StorageError {
    StorageError::Io(context("SQLite filesystem operation failed"))
}
fn sql(error: rusqlite::Error) -> StorageError {
    match error {
        rusqlite::Error::SqliteFailure(e, _)
            if matches!(
                e.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            ) =>
        {
            busy()
        }
        rusqlite::Error::SqliteFailure(e, _)
            if matches!(
                e.code,
                rusqlite::ErrorCode::SystemIoFailure
                    | rusqlite::ErrorCode::DiskFull
                    | rusqlite::ErrorCode::CannotOpen
            ) =>
        {
            StorageError::Io(context("SQLite operation failed"))
        }
        _ => corrupt("SQLite data or operation is invalid"),
    }
}
fn as_session(e: StorageError) -> SessionError {
    match e {
        StorageError::Closed(c) => SessionError::Closed(c),
        e => SessionError::Storage(e),
    }
}
fn codec_error(_: SessionError) -> StorageError {
    corrupt("stored value does not satisfy its codec")
}
fn now() -> Result<SafeUnsigned, StorageError> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| invalid("native time precedes epoch"))?
        .as_millis();
    SafeUnsigned::new(u64::try_from(value).map_err(|_| invalid("native time overflow"))?)
        .map_err(|_| invalid("native time overflow"))
}
fn add(a: SafeUnsigned, b: SafeUnsigned) -> Result<SafeUnsigned, StorageError> {
    SafeUnsigned::new(a.get() + b.get()).map_err(|_| invalid("safe integer exhausted"))
}
fn token() -> Result<StoreToken, StorageError> {
    use ring::rand::SecureRandom;
    let mut bytes = [0; 32];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| StorageError::Io(context("SQLite identity entropy unavailable")))?;
    Ok(StoreToken(bytes))
}
fn encode<T: serde::Serialize>(v: &T) -> Result<String, StorageError> {
    serde_json::to_string(v).map_err(|_| invalid("value cannot be serialized"))
}
fn scalar<T: serde::Serialize>(v: &T) -> Result<String, StorageError> {
    serde_json::to_value(v)
        .map_err(|_| invalid("invalid discriminator"))?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid("invalid discriminator"))
}
