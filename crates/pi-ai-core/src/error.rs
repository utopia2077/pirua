//! Redacted errors for portable AI runtime boundaries.

use std::error::Error;
use std::fmt;

use crate::auth::ModelsErrorCode;
use crate::event::AssistantMessageEventError;

/// Fixed terminal message used when caller cancellation wins stream settlement.
pub const REQUEST_CANCELLED_MESSAGE: &str = "request cancelled";
/// Fixed terminal message used when every producer disappears before settlement.
pub const STREAM_PRODUCER_DROPPED_MESSAGE: &str = "stream setup ended without a terminal event";

/// Marker returned by executor-neutral cancellation waits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cancelled;

impl fmt::Display for Cancelled {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(REQUEST_CANCELLED_MESSAGE)
    }
}

impl Error for Cancelled {}

/// Failure to admit an executor-neutral task.
pub struct SpawnError {
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl SpawnError {
    /// Creates a spawn failure without a backend source.
    pub const fn new() -> Self {
        Self { source: None }
    }

    /// Creates a spawn failure while retaining a source for explicit diagnosis.
    pub fn with_source(source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            source: Some(Box::new(source)),
        }
    }
}

impl Default for SpawnError {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SpawnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SpawnError")
            .field("source_present", &self.source.is_some())
            .finish()
    }
}

impl fmt::Display for SpawnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("task could not be spawned")
    }
}

impl Error for SpawnError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

/// Error creating a transient resolved request endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RequestEndpointError {
    /// The already-resolved endpoint was empty.
    Empty,
}

impl fmt::Display for RequestEndpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("resolved request endpoint must be nonempty")
    }
}

impl Error for RequestEndpointError {}

/// Rejected operation on an assistant stream producer.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StreamProtocolError {
    /// The event violated the terminal assistant-message contract.
    InvalidEvent(AssistantMessageEventError),
    /// The stream was already settled by another contender.
    AlreadySettled,
}

impl fmt::Display for StreamProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEvent(error) => error.fmt(formatter),
            Self::AlreadySettled => formatter.write_str("assistant stream is already settled"),
        }
    }
}

impl Error for StreamProtocolError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidEvent(error) => Some(error),
            Self::AlreadySettled => None,
        }
    }
}

impl From<AssistantMessageEventError> for StreamProtocolError {
    fn from(error: AssistantMessageEventError) -> Self {
        Self::InvalidEvent(error)
    }
}

/// Sanitized setup category converted into an in-band terminal event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StreamSetupFailure {
    /// Provider selection failed.
    UnknownProvider,
    /// Request authentication failed.
    Authentication,
    /// Adapter construction or setup failed.
    Adapter,
    /// The setup task could not be spawned.
    Spawn,
}

impl StreamSetupFailure {
    pub(crate) const fn terminal_message(self) -> &'static str {
        match self {
            Self::UnknownProvider => "stream provider is unavailable",
            Self::Authentication => "stream authentication failed",
            Self::Adapter => "stream adapter setup failed",
            Self::Spawn => "stream setup task could not be started",
        }
    }
}

/// Error returned when an optional adapter capability is absent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CapabilityError {
    /// The adapter does not implement the requested operation.
    Unsupported,
}

impl fmt::Display for CapabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported => formatter.write_str("adapter capability is unavailable"),
        }
    }
}

impl Error for CapabilityError {}

/// Redacted provider/model runtime error.
pub struct ModelsError {
    code: ModelsErrorCode,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl ModelsError {
    /// Creates a categorized error without a backend source.
    pub const fn new(code: ModelsErrorCode) -> Self {
        Self { code, source: None }
    }

    /// Creates a categorized error while retaining a source for explicit diagnosis.
    pub fn with_source(code: ModelsErrorCode, source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            code,
            source: Some(Box::new(source)),
        }
    }

    /// Returns the stable public category.
    pub const fn code(&self) -> ModelsErrorCode {
        self.code
    }
}

impl fmt::Debug for ModelsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelsError")
            .field("code", &self.code)
            .field("source_present", &self.source.is_some())
            .finish()
    }
}

impl fmt::Display for ModelsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self.code {
            ModelsErrorCode::ModelSource => "model source failed",
            ModelsErrorCode::ModelValidation => "model validation failed",
            ModelsErrorCode::Provider => "provider operation failed",
            ModelsErrorCode::Stream => "stream operation failed",
            ModelsErrorCode::Auth => "authentication failed",
            ModelsErrorCode::OAuth => "OAuth operation failed",
        };
        formatter.write_str(message)
    }
}

impl Error for ModelsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}
