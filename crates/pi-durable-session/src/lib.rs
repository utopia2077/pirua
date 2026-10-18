//! Durable committed records, scoped document transactions and native persistence.
//!
//! Execution policy belongs to a consumer. This crate owns only committed truth.
pub mod delta;
pub mod observation;
pub mod records;
pub mod session;
pub mod storage;

pub use delta::{Op, Path, Segment, apply};
pub use observation::*;
pub use records::*;
pub use session::*;
pub use storage::*;

use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Request,
    Validation,
    Backend,
    Closed,
    Failed,
    ReadAfterWrite,
    Definition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
}
pub type Result<T> = std::result::Result<T, Error>;
impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
    pub fn validation(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Validation, message)
    }
    pub fn request(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Request, message)
    }
    pub fn backend(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Backend, message)
    }
    pub(crate) fn fatal_read(&self) -> bool {
        !matches!(self.kind, ErrorKind::Request | ErrorKind::Definition)
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for Error {}
impl From<pi_sqlite::Error> for Error {
    fn from(e: pi_sqlite::Error) -> Self {
        Self::backend(e.to_string())
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::validation(e.to_string())
    }
}
