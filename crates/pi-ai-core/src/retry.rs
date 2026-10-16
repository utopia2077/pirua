//! Pure transient-provider error classification.

use std::sync::LazyLock;

use regex::{RegexSet, RegexSetBuilder};

use crate::overflow::compile_legacy_regex_set;
use crate::{AssistantMessage, StopReason};

pub(crate) const RETRYABLE_PATTERN_SOURCES: &[&str] = &[
    "overloaded",
    "server_busy",
    "servers are currently busy",
    "currently experiencing high demand",
    "model is at capacity",
    "(^|[^a-z0-9])(?:429|500|502|503|504|520|524)([^a-z0-9]|$)",
    "unable to forward upstream",
    "upstream.?request.?failed",
    "upstream_stream_read_error",
    "stream was interrupted",
    "http/?2 stream failed",
    "request to \\S+ failed, reason:",
    "stream disconnected before completion",
    "pending stream has been canceled",
    "subscription_sharing_usage_unavailable",
    "subscription_sharing_user_unavailable",
    "rate.?limit",
    "too many requests",
    "service.?(?:is.?)?(?:temporarily.?)?unavailable",
    "server.?error",
    "internal.?error",
    "provider.?returned.?(?:an.?)?error",
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

pub(crate) const NON_RETRYABLE_PATTERN_SOURCES: [&str; 10] = [
    "GoUsageLimitError",
    "FreeUsageLimitError",
    "Monthly usage limit reached",
    "available balance",
    "insufficient_quota",
    "out of budget",
    "quota exceeded",
    "billing",
    "subscription_sharing_usage_limit_exceeded",
    "subscription_sharing_workplace_individual_usage_limit_exceeded",
];

static RETRYABLE_PATTERNS: LazyLock<RegexSet> = LazyLock::new(|| {
    let patterns = RETRYABLE_PATTERN_SOURCES
        .iter()
        .map(|pattern| {
            if pattern.starts_with("(^|[^a-z0-9])") {
                pattern.replace("a-z0-9", "a-zA-Z0-9")
            } else {
                crate::overflow::translate_legacy_regex(pattern)
                    .expect("target pattern is within the portable subset")
            }
        })
        .collect::<Vec<_>>();
    RegexSetBuilder::new(patterns)
        .build()
        .expect("target retry patterns compile")
});
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
