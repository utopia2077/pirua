//! Pure transient-provider error classification.

use std::sync::LazyLock;

use regex::RegexSet;

use crate::overflow::compile_legacy_regex_set;
use crate::{AssistantMessage, StopReason};

pub(crate) const RETRYABLE_PATTERN_SOURCES: [&str; 41] = [
    "overloaded",
    "rate.?limit",
    "too many requests",
    "429",
    "500",
    "502",
    "503",
    "504",
    "524",
    "service.?unavailable",
    "server.?error",
    "internal.?error",
    "provider.?returned.?error",
    "exceeded request buffer limit while retrying upstream",
    "network.?error",
    "connection.?error",
    "connection.?refused",
    "connection.?lost",
    "other side closed",
    "fetch failed",
    "getaddrinfo",
    "ENOTFOUND",
    "EAI_AGAIN",
    "upstream.?connect",
    "reset before headers",
    "socket hang up",
    "socket connection was closed",
    "timed? out",
    "timeout",
    "terminated",
    "websocket.?closed",
    "websocket.?error",
    "ended without",
    "stream ended before message_stop",
    "stream ended before a terminal response event",
    "http2 request did not get a response",
    "retry delay",
    "you can retry your request",
    "try your request again",
    "please retry your request",
    "ResourceExhausted",
];

pub(crate) const NON_RETRYABLE_PATTERN_SOURCES: [&str; 8] = [
    "GoUsageLimitError",
    "FreeUsageLimitError",
    "Monthly usage limit reached",
    "available balance",
    "insufficient_quota",
    "out of budget",
    "quota exceeded",
    "billing",
];

static RETRYABLE_PATTERNS: LazyLock<RegexSet> =
    LazyLock::new(|| compile_legacy_regex_set(&RETRYABLE_PATTERN_SOURCES));
static NON_RETRYABLE_PATTERNS: LazyLock<RegexSet> =
    LazyLock::new(|| compile_legacy_regex_set(&NON_RETRYABLE_PATTERN_SOURCES));

/// Returns whether an assistant error is a retryable provider/transport failure.
pub fn is_retryable_assistant_error(message: &AssistantMessage) -> bool {
    if message.stop_reason != StopReason::Error {
        return false;
    }
    let Some(error_message) = message
        .error_message
        .as_deref()
        .filter(|message| !message.is_empty())
    else {
        return false;
    };
    !NON_RETRYABLE_PATTERNS.is_match(error_message) && RETRYABLE_PATTERNS.is_match(error_message)
}
