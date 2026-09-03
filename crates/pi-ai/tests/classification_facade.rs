use std::fmt::Debug;

use pi_ai::{
    AssistantMessage, OverflowPattern, SafeUnsigned, get_overflow_patterns, is_context_overflow,
    is_recoverable_length, is_retryable_assistant_error,
};

fn assert_pattern_contract<T: Debug + Send + Sync + 'static>() {}

fn message(stop_reason: &str, error_message: Option<&str>) -> AssistantMessage {
    let mut value = serde_json::json!({
        "role": "assistant",
        "content": [],
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": {
            "input": 0,
            "output": 0,
            "cacheRead": 0,
            "cacheWrite": 0,
            "totalTokens": 0,
            "cost": {
                "input": 0.0,
                "output": 0.0,
                "cacheRead": 0.0,
                "cacheWrite": 0.0,
                "total": 0.0
            }
        },
        "stopReason": stop_reason,
        "timestamp": 0
    });
    if let Some(error_message) = error_message {
        value["errorMessage"] = serde_json::Value::String(error_message.to_owned());
    }
    serde_json::from_value(value).expect("valid assistant fixture")
}

#[test]
fn classification_facade_reexports_exact_surface() {
    let _overflow: fn(&AssistantMessage, Option<SafeUnsigned>) -> bool = is_context_overflow;
    let _recoverable: fn(&AssistantMessage, SafeUnsigned) -> bool = is_recoverable_length;
    let _retryable: fn(&AssistantMessage) -> bool = is_retryable_assistant_error;
    let _patterns: fn() -> Vec<OverflowPattern> = get_overflow_patterns;
    let _matches: for<'a, 'b> fn(&'a OverflowPattern, &'b str) -> bool = OverflowPattern::is_match;
    assert_pattern_contract::<OverflowPattern>();

    let overflow = message("error", Some("token limit exceeded"));
    assert!(is_context_overflow(&overflow, None));
    let retryable = message("error", Some("temporary status 503"));
    assert!(is_retryable_assistant_error(&retryable));
    assert_eq!(get_overflow_patterns().len(), 25);
}
