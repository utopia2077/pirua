use serde_json::Value;
use std::{fmt, time::Duration};

pub type Result<T> = std::result::Result<T, Error>;

/// Errors retain protocol/HTTP identities without embedding credentials or URLs.
#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    Closed,
    Cancelled,
    Timeout(Duration),
    Protocol {
        code: i64,
        message: String,
        data: Option<Value>,
    },
    Invalid(String),
    Io(String),
    Http {
        status: u16,
        body: String,
    },
    AuthRequired {
        challenge: Option<String>,
    },
    SessionExpired,
    AuthorizationRequired {
        authorization_url: String,
    },
    OAuth {
        code: String,
        message: String,
    },
}

impl Error {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
    pub(crate) fn retryable(&self) -> bool {
        matches!(self, Self::Io(_))
            || matches!(self, Self::Http { status, .. } if *status == 408 || *status == 429 || *status >= 500)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "MCP connection closed"),
            Self::Cancelled => write!(f, "MCP request cancelled"),
            Self::Timeout(duration) => write!(f, "MCP request timed out after {duration:?}"),
            Self::Protocol { code, message, .. } => {
                write!(f, "MCP protocol error {code}: {message}")
            }
            Self::Invalid(message) => write!(f, "Invalid MCP data: {message}"),
            Self::Io(message) => write!(f, "MCP I/O error: {message}"),
            Self::Http { status, body } => write!(f, "MCP HTTP {status}: {body}"),
            Self::AuthRequired { .. } => write!(f, "MCP server requires authentication"),
            Self::SessionExpired => write!(f, "MCP session expired"),
            Self::AuthorizationRequired { .. } => write!(f, "MCP OAuth authorization required"),
            Self::OAuth { code, message } => write!(f, "MCP OAuth error {code}: {message}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Invalid(e.to_string())
    }
}
impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Self::Io(e.without_url().to_string())
    }
}
