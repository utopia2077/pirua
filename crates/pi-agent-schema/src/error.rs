//! Rust-only storage, session, and runtime error categories.

use std::fmt;
use std::sync::Arc;

const MAX_ERROR_MESSAGE_BYTES: usize = 512;
const MAX_ERROR_CODE_BYTES: usize = 64;

/// Error returned when bounded error context is invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorContextError {
    /// The operator-facing message was empty.
    Empty,
    /// The operator-facing message exceeded the schema bound.
    TooLong,
}

impl fmt::Display for ErrorContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("error context must be nonempty"),
            Self::TooLong => formatter.write_str("error context exceeds the bounded length"),
        }
    }
}

impl std::error::Error for ErrorContextError {}

/// Bounded, operator-facing context that must not contain user content or secrets.
/// Shared process-local error source retained without exposing it through Debug.
pub type SharedErrorSource = Arc<dyn std::error::Error + Send + Sync + 'static>;

/// Bounded operator-facing context with an optional redacted source chain.
#[derive(Clone)]
pub struct ErrorContext {
    message: String,
    source: Option<SharedErrorSource>,
}

impl ErrorContext {
    /// Creates bounded operator-facing context.
    pub fn new(message: impl Into<String>) -> Result<Self, ErrorContextError> {
        let message = message.into();
        if message.is_empty() {
            return Err(ErrorContextError::Empty);
        }
        if message.len() > MAX_ERROR_MESSAGE_BYTES {
            return Err(ErrorContextError::TooLong);
        }
        Ok(Self {
            message,
            source: None,
        })
    }

    /// Returns the bounded operator-facing message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Creates bounded context retaining a shared source chain.
    pub fn with_source(
        message: impl Into<String>,
        source: SharedErrorSource,
    ) -> Result<Self, ErrorContextError> {
        let mut context = Self::new(message)?;
        context.source = Some(source);
        Ok(context)
    }

    /// Returns the retained source without exposing it through Debug.
    pub fn source_ref(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

impl fmt::Debug for ErrorContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ErrorContext([REDACTED])")
    }
}

/// Error returned when an open backend error code is invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendErrorCodeError {
    /// The code was empty.
    Empty,
    /// The code exceeded the schema bound.
    TooLong,
}

impl fmt::Display for BackendErrorCodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("backend error code must be nonempty"),
            Self::TooLong => formatter.write_str("backend error code exceeds the bounded length"),
        }
    }
}

impl std::error::Error for BackendErrorCodeError {}

/// Bounded open backend error code.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BackendErrorCode(String);

impl BackendErrorCode {
    /// Creates a bounded open backend code.
    pub fn new(code: impl Into<String>) -> Result<Self, BackendErrorCodeError> {
        let code = code.into();
        if code.is_empty() {
            return Err(BackendErrorCodeError::Empty);
        }
        if code.len() > MAX_ERROR_CODE_BYTES {
            return Err(BackendErrorCodeError::TooLong);
        }
        Ok(Self(code))
    }

    /// Returns the open code.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BackendErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BackendErrorCode([REDACTED])")
    }
}

/// Stable Rust-only storage failure categories.
#[non_exhaustive]
#[derive(Clone)]
pub enum StorageError {
    /// The storage instance is closed.
    Closed(ErrorContext),
    /// A conditional write lost a conflict.
    Conflict(ErrorContext),
    /// Stored data is corrupt.
    Corruption(ErrorContext),
    /// Caller data violates the storage contract.
    InvalidData(ErrorContext),
    /// The backend is temporarily busy.
    Busy(ErrorContext),
    /// An input/output operation failed.
    Io(ErrorContext),
    /// The backend does not support the requested capability.
    Unsupported(ErrorContext),
    /// An open backend-specific category.
    Backend {
        /// Bounded open backend code.
        code: BackendErrorCode,
        /// Bounded content- and secret-free context.
        context: ErrorContext,
    },
}

impl StorageError {
    /// Returns the bounded operator-facing context.
    pub fn context(&self) -> &ErrorContext {
        match self {
            Self::Closed(context)
            | Self::Conflict(context)
            | Self::Corruption(context)
            | Self::InvalidData(context)
            | Self::Busy(context)
            | Self::Io(context)
            | Self::Unsupported(context)
            | Self::Backend { context, .. } => context,
        }
    }

    /// Returns the stable category string.
    pub fn category(&self) -> &'static str {
        match self {
            Self::Closed(_) => "closed",
            Self::Conflict(_) => "conflict",
            Self::Corruption(_) => "corruption",
            Self::InvalidData(_) => "invalid_data",
            Self::Busy(_) => "busy",
            Self::Io(_) => "io",
            Self::Unsupported(_) => "unsupported",
            Self::Backend { .. } => "backend",
        }
    }
}

impl fmt::Debug for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageError")
            .field("category", &self.category())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: {}",
            self.category(),
            self.context().message()
        )
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.context().source_ref()
    }
}

/// Stable Rust-only session failure categories.
#[non_exhaustive]
#[derive(Clone)]
pub enum SessionError {
    /// A storage operation failed.
    Storage(StorageError),
    /// A schema or codec invariant failed.
    Schema(ErrorContext),
    /// A custom message role has no registered schema.
    UnknownCustomRole(ErrorContext),
    /// The stored version is newer than this binary.
    NewerStorageVersion(ErrorContext),
    /// A migration failed.
    Migration(ErrorContext),
    /// The requested session or entry was not found.
    NotFound(ErrorContext),
    /// The session is closed.
    Closed(ErrorContext),
    /// The requested operation is invalid for current session state.
    InvalidOperation(ErrorContext),
}

impl SessionError {
    /// Returns the stable category string.
    pub fn category(&self) -> &'static str {
        match self {
            Self::Storage(_) => "storage",
            Self::Schema(_) => "schema",
            Self::UnknownCustomRole(_) => "unknown_custom_role",
            Self::NewerStorageVersion(_) => "newer_storage_version",
            Self::Migration(_) => "migration",
            Self::NotFound(_) => "not_found",
            Self::Closed(_) => "closed",
            Self::InvalidOperation(_) => "invalid_operation",
        }
    }

    fn message(&self) -> &str {
        match self {
            Self::Storage(error) => error.context().message(),
            Self::Schema(context)
            | Self::UnknownCustomRole(context)
            | Self::NewerStorageVersion(context)
            | Self::Migration(context)
            | Self::NotFound(context)
            | Self::Closed(context)
            | Self::InvalidOperation(context) => context.message(),
        }
    }
}

impl fmt::Debug for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionError")
            .field("category", &self.category())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.category(), self.message())
    }
}

impl std::error::Error for SessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Schema(context)
            | Self::UnknownCustomRole(context)
            | Self::NewerStorageVersion(context)
            | Self::Migration(context)
            | Self::NotFound(context)
            | Self::Closed(context)
            | Self::InvalidOperation(context) => context.source_ref(),
        }
    }
}

/// A runtime harness invariant or admitted-commit failure.
#[derive(Clone)]
pub struct HarnessFault {
    context: ErrorContext,
}

impl HarnessFault {
    /// Creates a bounded runtime fault.
    pub const fn new(context: ErrorContext) -> Self {
        Self { context }
    }
}

impl fmt::Debug for HarnessFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HarnessFault([REDACTED])")
    }
}

impl fmt::Display for HarnessFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.context.message())
    }
}

impl std::error::Error for HarnessFault {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.context.source_ref()
    }
}

/// Rejection after accepted work loses its process-local harness owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HarnessClosed;

impl fmt::Display for HarnessClosed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("harness closed")
    }
}

impl std::error::Error for HarnessClosed {}

/// Process-local tool execution/preparation failure converted to an in-band result.
#[derive(Clone)]
pub struct ToolExecutionError {
    context: ErrorContext,
}

impl ToolExecutionError {
    /// Creates a bounded content- and secret-free tool execution failure.
    pub const fn new(context: ErrorContext) -> Self {
        Self { context }
    }
}

impl fmt::Debug for ToolExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ToolExecutionError([REDACTED])")
    }
}

impl fmt::Display for ToolExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.context.message())
    }
}

impl std::error::Error for ToolExecutionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.context.source_ref()
    }
}

/// Ordinary isolated hook/event handler failure.
#[derive(Clone)]
pub struct HandlerError {
    context: ErrorContext,
}

impl HandlerError {
    /// Creates a bounded handler failure.
    pub const fn new(context: ErrorContext) -> Self {
        Self { context }
    }
}

impl fmt::Debug for HandlerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HandlerError([REDACTED])")
    }
}

impl fmt::Display for HandlerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.context.message())
    }
}

impl std::error::Error for HandlerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.context.source_ref()
    }
}

/// Synchronous hook registration failure such as a duplicate stable id.
#[derive(Clone)]
pub struct HookRegistrationError {
    context: ErrorContext,
}

impl HookRegistrationError {
    /// Creates a bounded registration failure.
    pub const fn new(context: ErrorContext) -> Self {
        Self { context }
    }
}

impl fmt::Debug for HookRegistrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HookRegistrationError([REDACTED])")
    }
}

impl fmt::Display for HookRegistrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.context.message())
    }
}

impl std::error::Error for HookRegistrationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.context.source_ref()
    }
}

/// Ordinary `run_when_idle` callback rejection.
#[derive(Clone)]
pub struct CallbackError {
    context: ErrorContext,
}

impl CallbackError {
    /// Creates a bounded callback error, optionally retaining a source through the context.
    pub const fn new(context: ErrorContext) -> Self {
        Self { context }
    }
}

impl fmt::Debug for CallbackError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CallbackError([REDACTED])")
    }
}

impl fmt::Display for CallbackError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.context.message())
    }
}

impl std::error::Error for CallbackError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.context.source_ref()
    }
}
