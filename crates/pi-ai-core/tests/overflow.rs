use std::collections::BTreeSet;
use std::fmt::Debug;

use pi_ai_core::{
    ApiId, AssistantMessage, AssistantRole, ModelId, OverflowPattern, ProviderId, SafeUnsigned,
    StopReason, Usage, get_overflow_patterns, is_context_overflow, is_recoverable_length,
    is_retryable_assistant_error,
};

const INVENTORY: &str = include_str!("../../../conformance/ai-domain/overflow-inventory.json");
const TEST_SOURCE: &str = include_str!("overflow.rs");

fn safe(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).expect("fixture is JavaScript-safe")
}

fn message(
    stop_reason: StopReason,
    error_message: Option<&str>,
    input: u64,
    output: u64,
    cache_read: u64,
) -> AssistantMessage {
    let mut usage = Usage::zero();
    usage.input = safe(input);
    usage.output = safe(output);
    usage.cache_read = safe(cache_read);
    AssistantMessage {
        role: AssistantRole::Assistant,
        content: Vec::new(),
        api: ApiId::new("test-api").expect("valid id"),
        provider: ProviderId::new("test-provider").expect("valid id"),
        model: ModelId::new("test-model").expect("valid id"),
        response_model: None,
        provider_thinking_level: None,
        thinking_level: None,
        duration_ms: None,
        response_id: None,
        diagnostics: None,
        usage,
        stop_reason,
        deferred: None,
        error_message: error_message.map(str::to_owned),
        raw_stop_reason: None,
        end_turn: None,
        timestamp: safe(0),
    }
}

const OVERFLOW_CASES: [(&str, &str); 25] = [
    (
        "overflow.error.anthropic-prompt-too-long",
        "PrOmPt Is ToO LoNg",
    ),
    (
        "overflow.error.anthropic-request-too-large",
        "request_too_large",
    ),
    (
        "overflow.error.bedrock-input-too-long",
        "input is too long for requested model",
    ),
    (
        "overflow.error.openai-context-window",
        "input exceeds the context window",
    ),
    (
        "overflow.error.openai-maximum-context-length",
        "requested count exceeds the model's maximum context length of 12,345 tokens",
    ),
    (
        "overflow.error.google-token-count",
        "input token count 999 exceeds the maximum",
    ),
    (
        "overflow.error.xai-prompt-length",
        "maximum prompt length is 123",
    ),
    (
        "overflow.error.groq-reduce-messages",
        "reduce the length of the messages",
    ),
    (
        "overflow.error.openrouter-context-length",
        "maximum context length is 123 tokens",
    ),
    (
        "overflow.error.poolside-input-length",
        "exceeds the maximum allowed input length of 1,234 tokens",
    ),
    (
        "overflow.error.together-context-length",
        "input (123 tokens) is longer than the model's context length (100 tokens)",
    ),
    (
        "overflow.error.copilot-token-limit",
        "exceeds the limit of 42",
    ),
    (
        "overflow.error.llama-context-size",
        "exceeds the available context size",
    ),
    (
        "overflow.error.lm-studio-context-length",
        "greater than the context length",
    ),
    (
        "overflow.error.minimax-context-window",
        "context window exceeds limit",
    ),
    (
        "overflow.error.kimi-model-token-limit",
        "exceeded model token limit",
    ),
    (
        "overflow.error.mistral-maximum-context",
        "too large for model with 7 maximum context length",
    ),
    (
        "overflow.error.ds4-configured-context",
        "prompt has 8 tokens, but the configured context size is 7 tokens",
    ),
    (
        "overflow.error.zai-context-window",
        "model_context_window_exceeded",
    ),
    (
        "overflow.error.ollama-prompt-too-long",
        "prompt too long; exceeded max context length",
    ),
    (
        "overflow.error.dashscope-input-range",
        "range of input length should be",
    ),
    (
        "overflow.error.generic-context-length",
        "context_length exceeded",
    ),
    ("overflow.error.generic-too-many-tokens", "too many tokens"),
    ("overflow.error.generic-token-limit", "token limit exceeded"),
    (
        "overflow.error.cerebras-no-body",
        "413 status code (no body)",
    ),
];

const EXPECTED_IDS: [&str; 43] = [
    "overflow.error.anthropic-prompt-too-long",
    "overflow.error.anthropic-request-too-large",
    "overflow.error.bedrock-input-too-long",
    "overflow.error.openai-context-window",
    "overflow.error.openai-maximum-context-length",
    "overflow.error.google-token-count",
    "overflow.error.xai-prompt-length",
    "overflow.error.groq-reduce-messages",
    "overflow.error.openrouter-context-length",
    "overflow.error.poolside-input-length",
    "overflow.error.together-context-length",
    "overflow.error.copilot-token-limit",
    "overflow.error.llama-context-size",
    "overflow.error.lm-studio-context-length",
    "overflow.error.minimax-context-window",
    "overflow.error.kimi-model-token-limit",
    "overflow.error.mistral-maximum-context",
    "overflow.error.ds4-configured-context",
    "overflow.error.zai-context-window",
    "overflow.error.ollama-prompt-too-long",
    "overflow.error.dashscope-input-range",
    "overflow.error.generic-context-length",
    "overflow.error.generic-too-many-tokens",
    "overflow.error.generic-token-limit",
    "overflow.error.cerebras-no-body",
    "overflow.exclusion.bedrock-throttling-service",
    "overflow.exclusion.rate-limit",
    "overflow.exclusion.too-many-requests",
    "overflow.structure.stop-reason",
    "overflow.structure.message-absent",
    "overflow.structure.message-empty",
    "overflow.structure.unmatched-message",
    "overflow.silent.input-over",
    "overflow.silent.cache-read-contributes",
    "overflow.silent.strict-threshold",
    "overflow.silent.context-and-reason-gate",
    "overflow.length.above-99-percent",
    "overflow.length.exact-99-percent",
    "overflow.length.below-99-percent",
    "overflow.length.nonzero-output",
    "overflow.length.context-and-reason-gate",
    "overflow.recoverable.boundary-matrix",
    "overflow.precedence.retry-overlap",
];

fn assert_pattern_contract<T: Debug + Send + Sync + 'static>() {}

#[test]
fn overflow_public_api_and_inventory_are_exact() {
    type IsOverflow = fn(&AssistantMessage, Option<SafeUnsigned>) -> bool;
    type IsRecoverable = fn(&AssistantMessage, SafeUnsigned) -> bool;
    type GetPatterns = fn() -> Vec<OverflowPattern>;
    let _overflow: IsOverflow = is_context_overflow;
    let _recoverable: IsRecoverable = is_recoverable_length;
    let _patterns: GetPatterns = get_overflow_patterns;
    let _is_match: for<'a, 'b> fn(&'a OverflowPattern, &'b str) -> bool = OverflowPattern::is_match;
    assert_pattern_contract::<OverflowPattern>();
    assert_eq!(
        format!("{:?}", get_overflow_patterns()[0]),
        "OverflowPattern"
    );

    let document: serde_json::Value = serde_json::from_str(INVENTORY).expect("valid inventory");
    assert_eq!(document["suite"], "overflow");
    assert_eq!(document["expectedRows"], 43);
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
        assert_eq!(row["owner"], "S03-overflow");
        assert!(matches!(
            row["polarity"].as_str(),
            Some("positive" | "negative" | "boundary" | "race")
        ));
        let source = row["source"].as_str().expect("source string");
        assert!(
            source.contains("@853a80d26c90a14c1886f0ebb8ffaae133ca2185")
                || source.ends_with("final-domain-runtime.md#failure-classifiers")
        );
        let binding = row["test"].as_str().expect("test binding");
        assert!(TEST_SOURCE.contains(&format!("fn {binding}(")));
        assert!(!row["expected"].as_str().expect("expected").is_empty());
    }
}

#[test]
fn overflow_error_pattern_rows() {
    let patterns = get_overflow_patterns();
    assert_eq!(patterns.len(), OVERFLOW_CASES.len());
    for (expected_index, (stable_id, error)) in OVERFLOW_CASES.iter().enumerate() {
        let matches = patterns
            .iter()
            .enumerate()
            .filter_map(|(index, pattern)| pattern.is_match(error).then_some(index))
            .collect::<Vec<_>>();
        assert_eq!(matches, vec![expected_index], "{stable_id}");
        assert!(
            is_context_overflow(&message(StopReason::Error, Some(error), 0, 0, 0), None),
            "{stable_id}"
        );
    }

    for error in [
        "400(no body)",
        "400 status code (no body)",
        "413\u{00a0}(no body)",
        "413\u{feff}(no body)",
        "413\u{2028}(no body)",
        "413\u{2029}(no body)",
    ] {
        assert!(is_context_overflow(
            &message(StopReason::Error, Some(error), 0, 0, 0),
            None
        ));
    }
    for error in ["429 (no body)", "429 status code (no body)"] {
        let message = message(StopReason::Error, Some(error), 0, 0, 0);
        assert!(!is_context_overflow(&message, None));
        assert!(is_retryable_assistant_error(&message));
    }
    assert!(!get_overflow_patterns()[6].is_match("maximum prompt length is ١"));
    assert!(!get_overflow_patterns()[5].is_match("input token count\nexceeds the maximum"));
    assert!(get_overflow_patterns()[5].is_match("input token count 😀 exceeds the maximum"));
}

#[test]
fn overflow_exclusion_rows() {
    for error in [
        "Throttling error: too many tokens",
        "Service unavailable: token limit exceeded",
        "rate limit; token limit exceeded",
        "too many requests; too many tokens",
    ] {
        assert!(!is_context_overflow(
            &message(StopReason::Error, Some(error), 0, 0, 0),
            None
        ));
    }
    assert!(is_context_overflow(
        &message(
            StopReason::Error,
            Some("prefix Service unavailable: token limit exceeded"),
            0,
            0,
            0,
        ),
        None,
    ));
}

#[test]
fn overflow_structural_negative_rows() {
    for reason in [
        StopReason::Pending,
        StopReason::Stop,
        StopReason::Length,
        StopReason::ToolUse,
        StopReason::Aborted,
        StopReason::Deferred,
    ] {
        assert!(!is_context_overflow(
            &message(reason, Some("token limit exceeded"), 0, 0, 0),
            None
        ));
    }
    for error in [None, Some(""), Some("unrecognized failure")] {
        assert!(!is_context_overflow(
            &message(StopReason::Error, error, 0, 0, 0),
            None
        ));
    }
}

#[test]
fn silent_overflow_rows() {
    assert!(is_context_overflow(
        &message(StopReason::Stop, None, 101, 0, 0),
        Some(safe(100))
    ));
    assert!(is_context_overflow(
        &message(StopReason::Stop, None, 50, 9_000, 51),
        Some(safe(100))
    ));
    for (input, cache_read) in [(100, 0), (99, 0), (50, 50)] {
        assert!(!is_context_overflow(
            &message(StopReason::Stop, None, input, 0, cache_read),
            Some(safe(100))
        ));
    }
    assert!(!is_context_overflow(
        &message(StopReason::Stop, None, 101, 0, 0),
        None
    ));
    assert!(!is_context_overflow(
        &message(StopReason::Stop, None, 101, 0, 0),
        Some(safe(0))
    ));
    assert!(!is_context_overflow(
        &message(StopReason::ToolUse, None, 101, 0, 0),
        Some(safe(100))
    ));
}

#[test]
fn length_overflow_rows() {
    assert!(is_context_overflow(
        &message(StopReason::Length, None, 100, 0, 0),
        Some(safe(100))
    ));
    assert!(is_context_overflow(
        &message(StopReason::Length, None, 99, 0, 0),
        Some(safe(100))
    ));
    assert!(!is_context_overflow(
        &message(StopReason::Length, None, 98, 0, 0),
        Some(safe(100))
    ));
    assert!(!is_context_overflow(
        &message(StopReason::Length, None, 100, 1, 0),
        Some(safe(100))
    ));
    for (reason, context) in [
        (StopReason::Length, None),
        (StopReason::Length, Some(safe(0))),
        (StopReason::Stop, Some(safe(100))),
    ] {
        assert!(!is_context_overflow(
            &message(reason, None, 100, 0, 0),
            context
        ));
    }

    assert!(is_context_overflow(
        &message(StopReason::Length, None, 8_917_127_262_193_581, 0, 0),
        Some(safe(9_007_199_254_740_991))
    ));
}

#[test]
fn recoverable_length_rows() {
    assert!(is_recoverable_length(
        &message(StopReason::Length, None, 0, 9, 0),
        safe(10)
    ));
    for (reason, output, desired) in [
        (StopReason::Length, 10, 10),
        (StopReason::Length, 11, 10),
        (StopReason::Length, 0, 0),
        (StopReason::Stop, 9, 10),
    ] {
        assert!(!is_recoverable_length(
            &message(reason, None, 0, output, 0),
            safe(desired)
        ));
    }
}

#[test]
fn overflow_retry_precedence_row() {
    let message = message(
        StopReason::Error,
        Some("token limit exceeded; transient 503"),
        0,
        0,
        0,
    );
    assert!(is_context_overflow(&message, None));
    assert!(is_retryable_assistant_error(&message));
}
