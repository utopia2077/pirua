use pi_ai_core::{
    Model, SafeUnsigned, StreamOptionsWire, ThinkingBudgets, ThinkingLevel, TranscriptContext,
};
use pi_ai_providers::api::simple_options::*;
use serde_json::{Value, json};

fn safe(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).unwrap()
}
fn model(window: u64) -> Model {
    serde_json::from_value(json!({"id":"model","name":"Model","api":"openai-responses","provider":"openai","baseUrl":"https://example.invalid","reasoning":true,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":window,"maxTokens":10000,"samplingParams":{"a":1,"b":2}})).unwrap()
}
fn context() -> TranscriptContext {
    serde_json::from_value(json!({"messages":[{"role":"user","content":"abcd","timestamp":0}]}))
        .unwrap()
}

#[test]
fn base_preserves_neutral_fields_and_merges_sampling_before_context_cap() {
    let wire: StreamOptionsWire = serde_json::from_value(json!({"timeoutMs":0,"maxRetries":2,"maxRetryDelayMs":4,"temperature":0,"samplingParams":{"b":null,"c":false},"maxTokens":10000,"transport":"websocket","cacheRetention":"none","sessionId":"","websocketConnectTimeoutMs":3,"metadata":{"x":1}})).unwrap();
    let normalized = build_base_options(&model(10000), &context(), &wire).unwrap();
    let mut expected = serde_json::to_value(&wire).unwrap();
    expected["samplingParams"] = json!({"a":1,"b":null,"c":false});
    expected["maxTokens"] = json!(5903);
    assert_eq!(serde_json::to_value(normalized).unwrap(), expected);
    assert_eq!(wire.max_tokens.unwrap().get(), 10000);
    assert_eq!(
        build_base_options(&model(5000), &context(), &StreamOptionsWire::default())
            .unwrap()
            .max_tokens
            .unwrap()
            .get(),
        903
    );
    for (window, requested, expected) in [
        (0, 0, 1),
        (0, 9999, 9999),
        (1, 0, 0),
        (1, 100, 1),
        (4097, 100, 1),
    ] {
        assert_eq!(
            clamp_max_tokens_to_context(&model(window), &context(), safe(requested))
                .unwrap()
                .get(),
            expected
        );
    }
}

#[test]
fn all_reasoning_levels_and_zero_custom_budget_match_pinned_source() {
    assert_eq!(MIN_ANSWER_TOKENS, 1024);
    assert_eq!(
        serde_json::to_value(DEFAULT_THINKING_BUDGETS).unwrap(),
        json!({"minimal":1024,"low":2048,"medium":8192,"high":16384})
    );
    assert_eq!(clamp_reasoning(None), None);
    for (level, expected) in [
        (ThinkingLevel::Minimal, 1024),
        (ThinkingLevel::Low, 2048),
        (ThinkingLevel::Medium, 8192),
        (ThinkingLevel::High, 16384),
        (ThinkingLevel::ExtraHigh, 16384),
        (ThinkingLevel::Max, 16384),
    ] {
        assert_eq!(thinking_budget_for_level(level, None).get(), expected);
        assert_eq!(
            clamp_reasoning(Some(level)),
            Some(
                if matches!(level, ThinkingLevel::ExtraHigh | ThinkingLevel::Max) {
                    ThinkingLevel::High
                } else {
                    level
                }
            )
        );
    }
    let custom: ThinkingBudgets = serde_json::from_value(json!({"minimal":0,"high":2})).unwrap();
    assert_eq!(
        thinking_budget_for_level(ThinkingLevel::Minimal, Some(&custom)).get(),
        0
    );
    assert_eq!(
        thinking_budget_for_level(ThinkingLevel::Low, Some(&custom)).get(),
        2048
    );
    assert_eq!(
        thinking_budget_for_level(ThinkingLevel::Max, Some(&custom)).get(),
        2
    );
}

#[test]
fn answer_room_and_safe_maximum_vectors_match_executed_pinned_oracle() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/simple-options/vectors.json"
    ))
    .unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let custom: Option<ThinkingBudgets> = case
            .get("custom")
            .map(|v| serde_json::from_value(v.clone()).unwrap());
        let result = adjust_max_tokens_for_thinking(
            case["base"].as_u64().map(safe),
            safe(case["ceiling"].as_u64().unwrap()),
            serde_json::from_value(case["level"].clone()).unwrap(),
            custom.as_ref(),
        );
        assert_eq!(
            json!({"maxTokens":result.max_tokens,"thinkingBudget":result.thinking_budget}),
            case["expected"]
        );
    }
    for (budget, ceiling, expected) in [
        (2000, 1000, 0),
        (2000, 1024, 0),
        (2000, 1025, 1),
        (2000, 4096, 2000),
    ] {
        assert_eq!(
            clamp_thinking_budget_to_answer_room(safe(budget), safe(ceiling)).get(),
            expected
        );
    }
}
