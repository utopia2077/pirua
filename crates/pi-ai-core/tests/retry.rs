use std::collections::BTreeSet;

use pi_ai_core::{
    ApiId, AssistantMessage, AssistantRole, ModelId, ProviderId, SafeUnsigned, StopReason, Usage,
    is_retryable_assistant_error,
};

const INVENTORY: &str = include_str!("../../../conformance/ai-domain/retry-inventory.json");
const TEST_SOURCE: &str = include_str!("retry.rs");

fn safe(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).expect("fixture is JavaScript-safe")
}

fn message(stop_reason: StopReason, error_message: Option<&str>) -> AssistantMessage {
    AssistantMessage {
        role: AssistantRole::Assistant,
        content: Vec::new(),
        api: ApiId::new("test-api").expect("valid id"),
        provider: ProviderId::new("test-provider").expect("valid id"),
        model: ModelId::new("test-model").expect("valid id"),
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: Usage::zero(),
        stop_reason,
        deferred: None,
        error_message: error_message.map(str::to_owned),
        raw_stop_reason: None,
        end_turn: None,
        timestamp: safe(0),
    }
}

const RETRYABLE_CASES: [(&str, &str); 41] = [
    ("retry.transient.overloaded", "provider overloaded"),
    ("retry.transient.rate-limit", "rate-limit"),
    ("retry.transient.too-many-requests", "too many requests"),
    ("retry.transient.http-429", "status 429"),
    ("retry.transient.http-500", "status 500"),
    ("retry.transient.http-502", "status 502"),
    ("retry.transient.http-503", "status 503"),
    ("retry.transient.http-504", "status 504"),
    ("retry.transient.http-524", "status 524"),
    ("retry.transient.service-unavailable", "service unavailable"),
    ("retry.transient.server-error", "server-error"),
    ("retry.transient.internal-error", "internal_error"),
    (
        "retry.transient.provider-returned-error",
        "providerXreturnedYerror",
    ),
    (
        "retry.transient.request-buffer-limit",
        "exceeded request buffer limit while retrying upstream",
    ),
    ("retry.transient.network-error", "network:error"),
    ("retry.transient.connection-error", "connection/error"),
    ("retry.transient.connection-refused", "connection refused"),
    ("retry.transient.connection-lost", "connection lost"),
    ("retry.transient.other-side-closed", "other side closed"),
    ("retry.transient.fetch-failed", "fetch failed"),
    ("retry.transient.dns-getaddrinfo", "getaddrinfo"),
    ("retry.transient.dns-enotfound", "enotfound"),
    ("retry.transient.dns-eai-again", "eai_again"),
    ("retry.transient.upstream-connect", "upstream-connect"),
    (
        "retry.transient.reset-before-headers",
        "reset before headers",
    ),
    ("retry.transient.socket-hang-up", "socket hang up"),
    (
        "retry.transient.socket-connection-closed",
        "socket connection was closed",
    ),
    ("retry.transient.timed-out", "timed out"),
    ("retry.transient.timeout", "timeout"),
    ("retry.transient.terminated", "terminated"),
    ("retry.transient.websocket-closed", "websocket_closed"),
    ("retry.transient.websocket-error", "websocket:error"),
    ("retry.transient.ended-without", "ended without"),
    (
        "retry.transient.anthropic-message-stop",
        "stream ended before message_stop",
    ),
    (
        "retry.transient.responses-terminal-event",
        "stream ended before a terminal response event",
    ),
    (
        "retry.transient.http2-no-response",
        "http2 request did not get a response",
    ),
    ("retry.transient.retry-delay", "retry delay"),
    (
        "retry.transient.guidance-you-can-retry",
        "you can retry your request",
    ),
    (
        "retry.transient.guidance-try-again",
        "try your request again",
    ),
    (
        "retry.transient.guidance-please-retry",
        "please retry your request",
    ),
    (
        "retry.transient.grpc-resource-exhausted",
        "resourceexhausted",
    ),
];

const NON_RETRYABLE_CASES: [(&str, &str); 8] = [
    ("retry.nonretryable.opencode-go-limit", "GoUsageLimitError"),
    (
        "retry.nonretryable.opencode-free-limit",
        "FreeUsageLimitError",
    ),
    (
        "retry.nonretryable.monthly-usage-limit",
        "Monthly usage limit reached",
    ),
    ("retry.nonretryable.available-balance", "available balance"),
    (
        "retry.nonretryable.insufficient-quota",
        "insufficient_quota",
    ),
    ("retry.nonretryable.out-of-budget", "out of budget"),
    ("retry.nonretryable.quota-exceeded", "quota exceeded"),
    ("retry.nonretryable.billing", "billing"),
];

const EXPECTED_IDS: [&str; 54] = [
    "retry.transient.overloaded",
    "retry.transient.rate-limit",
    "retry.transient.too-many-requests",
    "retry.transient.http-429",
    "retry.transient.http-500",
    "retry.transient.http-502",
    "retry.transient.http-503",
    "retry.transient.http-504",
    "retry.transient.http-524",
    "retry.transient.service-unavailable",
    "retry.transient.server-error",
    "retry.transient.internal-error",
    "retry.transient.provider-returned-error",
    "retry.transient.request-buffer-limit",
    "retry.transient.network-error",
    "retry.transient.connection-error",
    "retry.transient.connection-refused",
    "retry.transient.connection-lost",
    "retry.transient.other-side-closed",
    "retry.transient.fetch-failed",
    "retry.transient.dns-getaddrinfo",
    "retry.transient.dns-enotfound",
    "retry.transient.dns-eai-again",
    "retry.transient.upstream-connect",
    "retry.transient.reset-before-headers",
    "retry.transient.socket-hang-up",
    "retry.transient.socket-connection-closed",
    "retry.transient.timed-out",
    "retry.transient.timeout",
    "retry.transient.terminated",
    "retry.transient.websocket-closed",
    "retry.transient.websocket-error",
    "retry.transient.ended-without",
    "retry.transient.anthropic-message-stop",
    "retry.transient.responses-terminal-event",
    "retry.transient.http2-no-response",
    "retry.transient.retry-delay",
    "retry.transient.guidance-you-can-retry",
    "retry.transient.guidance-try-again",
    "retry.transient.guidance-please-retry",
    "retry.transient.grpc-resource-exhausted",
    "retry.nonretryable.opencode-go-limit",
    "retry.nonretryable.opencode-free-limit",
    "retry.nonretryable.monthly-usage-limit",
    "retry.nonretryable.available-balance",
    "retry.nonretryable.insufficient-quota",
    "retry.nonretryable.out-of-budget",
    "retry.nonretryable.quota-exceeded",
    "retry.nonretryable.billing",
    "retry.structure.non-error-reasons",
    "retry.structure.message-absent",
    "retry.structure.message-empty",
    "retry.structure.unmatched-message",
    "retry.structure.whitespace-message",
];

#[test]
fn retry_public_api_and_inventory_are_exact() {
    let _classifier: fn(&AssistantMessage) -> bool = is_retryable_assistant_error;
    let document: serde_json::Value = serde_json::from_str(INVENTORY).expect("valid inventory");
    assert_eq!(document["suite"], "retry");
    assert_eq!(document["expectedRows"], 54);
    let rows = document["rows"].as_array().expect("row array");
    assert_eq!(rows.len(), EXPECTED_IDS.len());
    let expected_keys = ["expected", "id", "owner", "polarity", "source", "test"]
        .into_iter()
        .collect::<BTreeSet<_>>();
    for (row, expected_id) in rows.iter().zip(EXPECTED_IDS) {
        let row = row.as_object().expect("row object");
        assert_eq!(
            row.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            expected_keys
        );
        assert_eq!(row["id"], expected_id);
        assert_eq!(row["owner"], "S03-retry");
        assert!(matches!(
            row["polarity"].as_str(),
            Some("positive" | "negative" | "boundary" | "race")
        ));
        assert!(
            row["source"]
                .as_str()
                .expect("source")
                .contains("@853a80d26c90a14c1886f0ebb8ffaae133ca2185")
        );
        let binding = row["test"].as_str().expect("test binding");
        assert!(TEST_SOURCE.contains(&format!("fn {binding}(")));
        assert!(!row["expected"].as_str().expect("expected").is_empty());
    }
}

#[test]
fn retryable_marker_rows() {
    for (stable_id, error) in RETRYABLE_CASES {
        assert!(
            is_retryable_assistant_error(&message(StopReason::Error, Some(error))),
            "{stable_id}"
        );
    }

    for error in ["ratelimit", "rate limit", "rate\u{00a0}limit", "rate-limit"] {
        assert!(is_retryable_assistant_error(&message(
            StopReason::Error,
            Some(error)
        )));
    }
    for error in [
        "rate😀limit",
        "rate..limit",
        "rate\nlimit",
        "rate\rlimit",
        "rate\u{2028}limit",
        "rate\u{2029}limit",
        "ſervice unavailable",
        "networK error",
        "status ٤٢٩",
    ] {
        assert!(
            !is_retryable_assistant_error(&message(StopReason::Error, Some(error))),
            "legacy regex boundary {error:?}"
        );
    }
    assert!(is_retryable_assistant_error(&message(
        StopReason::Error,
        Some("SeRvIcE UnAvAiLaBlE")
    )));
    assert!(is_retryable_assistant_error(&message(
        StopReason::Error,
        Some("time out")
    )));
}

#[test]
fn non_retryable_precedence_rows() {
    for (stable_id, marker) in NON_RETRYABLE_CASES {
        let error = format!("{marker}; transient 503");
        assert!(
            !is_retryable_assistant_error(&message(StopReason::Error, Some(&error))),
            "{stable_id}"
        );
    }
}

#[test]
fn retry_structural_negative_rows() {
    for reason in [
        StopReason::Pending,
        StopReason::Stop,
        StopReason::Length,
        StopReason::ToolUse,
        StopReason::Aborted,
        StopReason::Deferred,
    ] {
        assert!(!is_retryable_assistant_error(&message(reason, Some("503"))));
    }
    for error in [None, Some(""), Some("unknown failure"), Some(" \t ")] {
        assert!(!is_retryable_assistant_error(&message(
            StopReason::Error,
            error
        )));
    }
}
