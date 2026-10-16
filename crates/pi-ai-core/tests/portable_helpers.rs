use std::collections::{BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use pi_ai_core::{
    AssistantContent, BoxFuture, CancellationToken, Cancelled, Clock, ContentTextInput, Context,
    ContextEstimateInput, FiniteF64, InputContent, JsonObject, JsonParseErrorCode, JsonSchema,
    JsonValue, Message, SafeUnsigned, TextAndImageContentRef, Tool, ToolName, Usage, UsageCost,
    UuidV7Entropy, UuidV7EntropyError, UuidV7ErrorCode, UuidV7Generator, calculate_context_tokens,
    content_text, content_text_with_separator, estimate_context_tokens, estimate_message_tokens,
    estimate_text_and_image_content_tokens, estimate_text_tokens, parse_json_with_repair,
    parse_streaming_json, repair_json, short_hash,
};

fn safe(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).expect("fixture is JavaScript-safe")
}

fn finite(value: f64) -> FiniteF64 {
    FiniteF64::new(value).expect("fixture is finite")
}

fn usage(total: u64, input: u64, output: u64, cache_read: u64, cache_write: u64) -> Usage {
    Usage {
        input: safe(input),
        output: safe(output),
        cache_read: safe(cache_read),
        cache_write: safe(cache_write),
        cache_write_1h: None,
        reasoning: None,
        total_tokens: safe(total),
        cost: UsageCost {
            input: finite(0.0),
            output: finite(0.0),
            cache_read: finite(0.0),
            cache_write: finite(0.0),
            total: finite(0.0),
        },
    }
}

fn message(value: serde_json::Value) -> Message {
    serde_json::from_value(value).expect("valid message fixture")
}

fn assistant(timestamp: u64, total: u64, stop_reason: &str) -> Message {
    message(serde_json::json!({
        "role": "assistant",
        "content": [{"type":"text", "text":"kept"}],
        "api": "test-api",
        "provider": "test-provider",
        "model": "test-model",
        "usage": {
            "input": total,
            "output": 0,
            "cacheRead": 0,
            "cacheWrite": 0,
            "totalTokens": total,
            "cost": {"input":0.0,"output":0.0,"cacheRead":0.0,"cacheWrite":0.0,"total":0.0}
        },
        "stopReason": stop_reason,
        "timestamp": timestamp
    }))
}

fn user(timestamp: u64, text: &str) -> Message {
    message(serde_json::json!({
        "role":"user",
        "content":text,
        "timestamp":timestamp
    }))
}

fn tool(name: &str, description: &str) -> Tool {
    Tool {
        name: ToolName::new(name).expect("valid tool name"),
        description: description.to_owned(),
        parameters: JsonSchema(JsonObject::default()),
        constrained_sampling: None,
    }
}

#[test]
fn context_usage_prefers_nonzero_total_and_falls_back_to_components() {
    assert_eq!(
        calculate_context_tokens(&usage(99, 1, 2, 3, 4)),
        Ok(safe(99))
    );
    assert_eq!(
        calculate_context_tokens(&usage(0, 1, 2, 3, 4)),
        Ok(safe(10))
    );
    assert!(calculate_context_tokens(&usage(0, SafeUnsigned::MAX.get(), 1, 0, 0)).is_err());
}

#[test]
fn text_and_image_estimates_use_utf16_units_and_fixed_image_cost() {
    assert_eq!(estimate_text_tokens("abc😀"), Ok(safe(2)));
    let blocks: Vec<InputContent> = serde_json::from_value(serde_json::json!([
        {"type":"text","text":"a"},
        {"type":"image","data":"ignored","mimeType":"image/png"},
        {"type":"text","text":"😀"}
    ]))
    .expect("content fixture");
    assert_eq!(
        estimate_text_and_image_content_tokens(TextAndImageContentRef::Blocks(&blocks)),
        Ok(safe(1_201))
    );
}

#[test]
fn message_estimates_cover_each_role_and_assistant_block() {
    assert_eq!(estimate_message_tokens(&user(1, "abcde")), Ok(safe(2)));
    let tool_result = message(serde_json::json!({
        "role":"toolResult",
        "toolCallId":"call",
        "toolName":"tool",
        "content":[{"type":"image","data":"ignored","mimeType":"image/png"}],
        "isError":false,
        "timestamp":1
    }));
    assert_eq!(estimate_message_tokens(&tool_result), Ok(safe(1_200)));
    let assistant = message(serde_json::json!({
        "role":"assistant",
        "content":[
            {"type":"text","text":"a"},
            {"type":"thinking","thinking":"😀"},
            {"type":"toolCall","id":"call","name":"xy","arguments":{}}
        ],
        "api":"api","provider":"provider","model":"model",
        "usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,
          "cost":{"input":0.0,"output":0.0,"cacheRead":0.0,"cacheWrite":0.0,"total":0.0}},
        "stopReason":"stop","timestamp":1
    }));
    assert_eq!(estimate_message_tokens(&assistant), Ok(safe(2)));

    let numeric_tool_call = message(serde_json::json!({
        "role":"assistant",
        "content":[{"type":"toolCall","id":"call","name":"x","arguments":{
            "negativeZero":-0.0,"small":1e-7,"large":1e21
        }}],
        "api":"api","provider":"provider","model":"model",
        "usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,
          "cost":{"input":0.0,"output":0.0,"cacheRead":0.0,"cacheWrite":0.0,"total":0.0}},
        "stopReason":"stop","timestamp":1
    }));
    let canonical = r#"{"large":1e+21,"negativeZero":0,"small":1e-7}"#;
    let expected = (1 + u64::try_from(canonical.encode_utf16().count())
        .expect("fixture length fits u64"))
    .div_ceil(4);
    assert_eq!(
        estimate_message_tokens(&numeric_tool_call),
        Ok(safe(expected))
    );
}

#[test]
fn context_without_checkpoint_adds_system_tools_and_all_messages() {
    let tools = vec![tool("echo", "d")];
    let tool_only = estimate_context_tokens(ContextEstimateInput::Context(&Context {
        system_prompt: None,
        messages: Vec::new(),
        tools: Some(tools.clone()),
    }))
    .expect("tool estimate");
    let estimate = estimate_context_tokens(ContextEstimateInput::Context(&Context {
        system_prompt: Some("1234".to_owned()),
        messages: vec![user(1, "1234")],
        tools: Some(tools),
    }))
    .expect("context estimate");
    assert_eq!(estimate.last_usage_index, None);
    assert_eq!(estimate.usage_tokens, safe(0));
    assert_eq!(estimate.tokens.get(), tool_only.tokens.get() + 2);
    assert_eq!(estimate.trailing_tokens, estimate.tokens);
}

#[test]
fn context_checkpoint_reuses_usage_and_estimates_only_trailing_content() {
    let estimate = estimate_context_tokens(ContextEstimateInput::Context(&Context {
        system_prompt: Some("ignored".to_owned()),
        messages: vec![assistant(100, 2_000, "stop"), user(200, "tail")],
        tools: Some(vec![tool("ignored", "ignored")]),
    }))
    .expect("context estimate");
    assert_eq!(
        estimate,
        pi_ai_core::ContextUsageEstimate {
            tokens: safe(2_001),
            usage_tokens: safe(2_000),
            trailing_tokens: safe(1),
            last_usage_index: Some(safe(0)),
        }
    );
}

#[test]
fn stale_failed_aborted_and_zero_usage_are_not_checkpoints() {
    let stale = Context {
        system_prompt: Some("system".to_owned()),
        messages: vec![
            user(200, "summary"),
            assistant(100, 9_500, "stop"),
            user(300, &"x".repeat(4_000)),
        ],
        tools: None,
    };
    assert_eq!(
        estimate_context_tokens(ContextEstimateInput::Context(&stale)).expect("estimate"),
        pi_ai_core::ContextUsageEstimate {
            tokens: safe(1_005),
            usage_tokens: safe(0),
            trailing_tokens: safe(1_005),
            last_usage_index: None,
        }
    );
    for invalid in [
        assistant(100, 1_000, "error"),
        assistant(100, 1_000, "aborted"),
        assistant(100, 0, "stop"),
    ] {
        let estimate =
            estimate_context_tokens(ContextEstimateInput::Messages(&[invalid])).expect("estimate");
        assert_eq!(estimate.last_usage_index, None);
        assert_eq!(estimate.usage_tokens, safe(0));
    }
}

#[test]
fn newer_assistant_usage_restores_checkpoint() {
    let messages = vec![
        user(200, "summary"),
        assistant(100, 9_500, "stop"),
        user(300, "new prompt"),
        assistant(400, 2_000, "stop"),
        user(500, "tail"),
    ];
    let estimate =
        estimate_context_tokens(ContextEstimateInput::Messages(&messages)).expect("estimate");
    assert_eq!(estimate.tokens, safe(2_001));
    assert_eq!(estimate.usage_tokens, safe(2_000));
    assert_eq!(estimate.trailing_tokens, safe(1));
    assert_eq!(estimate.last_usage_index, Some(safe(3)));

    let equal_timestamp = vec![user(100, "prefix"), assistant(100, 20, "stop")];
    assert_eq!(
        estimate_context_tokens(ContextEstimateInput::Messages(&equal_timestamp))
            .expect("estimate")
            .last_usage_index,
        Some(safe(1))
    );
}

#[test]
fn post_checkpoint_markers_add_only_named_available_tool_definitions() {
    let selected = tool("later", "selected");
    let selected_tokens = estimate_context_tokens(ContextEstimateInput::Context(&Context {
        system_prompt: None,
        messages: Vec::new(),
        tools: Some(vec![selected.clone()]),
    }))
    .expect("tool estimate")
    .tokens;
    let marker = message(serde_json::json!({
        "role":"system","content":"", "toolsAdded":[selected.clone()], "timestamp":2
    }));
    let estimate = estimate_context_tokens(ContextEstimateInput::Context(&Context {
        system_prompt: Some("ignored".to_owned()),
        messages: vec![assistant(1, 100, "stop"), marker],
        tools: Some(vec![tool("early", "ignored"), selected]),
    }))
    .expect("context estimate");
    assert_eq!(estimate.usage_tokens, safe(100));
    assert_eq!(estimate.trailing_tokens, selected_tokens);
    assert_eq!(
        estimate.tokens,
        SafeUnsigned::new(100 + selected_tokens.get()).expect("safe")
    );
}

#[test]
fn repair_json_preserves_valid_json_and_escape_sequences() {
    for valid in [
        r#"{"text":"line\nquote\"slash\\solidus\/unicode\u0041"}"#,
        r#"{"array":[1,true,null],"object":{}}"#,
    ] {
        assert_eq!(repair_json(valid), valid);
    }
}

#[test]
fn repair_json_escapes_only_raw_string_control_characters() {
    let input = "{\n\"value\":\"a\n\tb\u{0000}\"\n}";
    assert_eq!(repair_json(input), "{\n\"value\":\"a\\n\\tb\\u0000\"\n}");
}

#[test]
fn repair_json_doubles_invalid_and_trailing_backslashes() {
    assert_eq!(repair_json(r#"{"value":"bad\q"}"#), r#"{"value":"bad\\q"}"#);
    assert_eq!(repair_json(r#""abc\"#), r#""abc\\"#);
    assert_eq!(repair_json(r#""abc\u12"#), r#""abc\u12"#);
}

#[test]
fn parse_json_uses_original_then_repaired_input_without_mutation() {
    let valid = r#"{"value":"ok"}"#.to_owned();
    let before = valid.clone();
    let parsed: serde_json::Value = parse_json_with_repair(&valid).expect("valid JSON");
    assert_eq!(parsed, serde_json::json!({"value":"ok"}));
    assert_eq!(valid, before);

    let repaired: serde_json::Value =
        parse_json_with_repair(r#"{"value":"bad\q"}"#).expect("repair succeeds");
    assert_eq!(repaired, serde_json::json!({"value":r"bad\q"}));
}

#[test]
fn invalid_json_returns_typed_bounded_error_without_input() {
    let error =
        parse_json_with_repair::<serde_json::Value>(r#"{"JSON_PARSE_SECRET_SENTINEL": invalid}"#)
            .expect_err("invalid JSON");
    assert_eq!(error.code(), JsonParseErrorCode::InvalidJson);
    assert_eq!(error.to_string(), "invalid JSON");
    assert!(!format!("{error:?} {error}").contains("JSON_PARSE_SECRET_SENTINEL"));
    assert!(Error::source(&error).is_none());
}

fn empty_json_object() -> JsonValue {
    JsonValue::Object(JsonObject::default())
}

#[test]
fn streaming_json_empty_null_and_malformed_fallback_matrix() {
    for input in [None, Some(""), Some(" \u{feff}\n"), Some("wrong")] {
        assert_eq!(parse_streaming_json(input), empty_json_object());
    }
    assert_eq!(parse_streaming_json(Some("null")), JsonValue::Null);
    assert_eq!(parse_streaming_json(Some("nu")), empty_json_object());
    assert_eq!(
        parse_streaming_json(Some(r#"{"complete":true}"#)),
        serde_json::from_str(r#"{"complete":true}"#).expect("fixture")
    );
}

#[test]
fn streaming_json_recovers_partial_collections_atoms_and_invalid_escapes() {
    assert_eq!(
        parse_streaming_json(Some(r#"[{"key":"value"},{"nested":[1,2,"tail"#)),
        serde_json::from_str(r#"[{"key":"value"},{"nested":[1,2,"tail"]}]"#).expect("fixture")
    );
    assert_eq!(parse_streaming_json(Some("tr")), JsonValue::Bool(true));
    assert_eq!(
        parse_streaming_json(Some("1e+")),
        serde_json::from_str("1").expect("fixture")
    );
    assert_eq!(parse_streaming_json(Some("123.")), empty_json_object());
    assert_eq!(
        parse_streaming_json(Some(r#"{"value":"bad\q"}"#)),
        serde_json::from_str(r#"{"value":"bad\\q"}"#).expect("fixture")
    );
    assert_eq!(
        parse_streaming_json(Some(r#"{"a":1,wrong"#)),
        serde_json::from_str(r#"{"a":1}"#).expect("fixture")
    );
    assert_eq!(
        parse_streaming_json(Some(r#"{"a":1,"bad\q":2"#)),
        serde_json::from_str(r#"{"a":1}"#).expect("fixture")
    );
    for (partial, recovered) in [
        (r#"{"a":trueX"#, r#"{"a":true}"#),
        (r#"{"a":"x"X"#, r#"{"a":"x"}"#),
        (r#"[trueX"#, r#"[true]"#),
        (r#"{"a":true "b":false}"#, r#"{"a":true,"b":false}"#),
        (r#"[true false]"#, r#"[true,false]"#),
    ] {
        assert_eq!(
            parse_streaming_json(Some(partial)),
            serde_json::from_str(recovered).expect("pinned partial-json prefix fixture")
        );
    }
    for rejected in [
        r#"{"a":1,"a":2"#,
        "9007199254740992",
        "NaN",
        "Infinity",
        "-Infinity",
    ] {
        assert_eq!(parse_streaming_json(Some(rejected)), empty_json_object());
    }

    let depth_128 = format!("{}0", "[".repeat(128));
    assert!(matches!(
        parse_streaming_json(Some(&depth_128)),
        JsonValue::Array(_)
    ));
    let depth_129 = format!("{}0", "[".repeat(129));
    assert_eq!(parse_streaming_json(Some(&depth_129)), empty_json_object());

    const LIMIT: usize = 256 * 1024;
    let complete_at_limit = format!(r#"{{"x":"{}"}}"#, "a".repeat(LIMIT - 8));
    assert!(matches!(
        parse_streaming_json(Some(&complete_at_limit)),
        JsonValue::Object(_)
    ));
    let complete_over_limit = format!(r#"{{"x":"{}"}}"#, "a".repeat(LIMIT - 7));
    assert_eq!(
        parse_streaming_json(Some(&complete_over_limit)),
        empty_json_object()
    );
    let partial_at_limit = format!(r#"{{"x":"{}"#, "a".repeat(LIMIT - 6));
    assert!(matches!(
        parse_streaming_json(Some(&partial_at_limit)),
        JsonValue::Object(_)
    ));
    let partial_over_limit = format!(r#"{{"x":"{}"#, "a".repeat(LIMIT - 5));
    assert_eq!(
        parse_streaming_json(Some(&partial_over_limit)),
        empty_json_object()
    );
    let repair_would_expand = format!(r#"{{"x":"{}\q"}}"#, "a".repeat(LIMIT - 10));
    assert_eq!(repair_would_expand.len(), LIMIT);
    assert!(repair_json(&repair_would_expand).len() > LIMIT);
    assert_eq!(
        parse_streaming_json(Some(&repair_would_expand)),
        empty_json_object()
    );
}

#[test]
fn content_text_preserves_strings_and_supports_default_and_custom_separators() {
    assert_eq!(content_text(ContentTextInput::Text("plain")), "plain");
    let blocks: Vec<InputContent> = serde_json::from_value(serde_json::json!([
        {"type":"text","text":"first"},
        {"type":"text","text":"second"}
    ]))
    .expect("blocks");
    assert_eq!(
        content_text(ContentTextInput::Input(&blocks)),
        "first\nsecond"
    );
    assert_eq!(
        content_text_with_separator(ContentTextInput::Input(&blocks), " | "),
        "first | second"
    );
}

#[test]
fn content_text_selects_only_text_blocks_from_input_and_assistant_content() {
    let input: Vec<InputContent> = serde_json::from_value(serde_json::json!([
        {"type":"image","data":"secret","mimeType":"image/png"},
        {"type":"text","text":"visible"}
    ]))
    .expect("input");
    assert_eq!(content_text(ContentTextInput::Input(&input)), "visible");
    let assistant: Vec<AssistantContent> = serde_json::from_value(serde_json::json!([
        {"type":"thinking","thinking":"hidden"},
        {"type":"text","text":"shown"},
        {"type":"toolCall","id":"call","name":"tool","arguments":{}}
    ]))
    .expect("assistant");
    assert_eq!(
        content_text(ContentTextInput::Assistant(&assistant)),
        "shown"
    );
}

#[test]
fn short_hash_matches_ascii_wrapping_and_base36_vectors() {
    for (input, expected) in [
        ("", "k4n83c7h0j2b"),
        ("a", "m8735310ae7sx"),
        ("hello", "1h6qa0qrowduu"),
        ("The quick brown fox", "na9l2t124bi96"),
    ] {
        assert_eq!(short_hash(input), expected);
    }
}

#[test]
fn short_hash_hashes_utf16_code_units_for_non_bmp_text() {
    assert_eq!(short_hash("😀"), "13wj7r7usi372");
    assert_eq!(short_hash("a😀b"), "12yrce3kjl8pw");
    assert_ne!(short_hash("😀"), short_hash("�"));
}

struct QueueClock(Mutex<VecDeque<u64>>);

impl Clock for QueueClock {
    fn now_unix_ms(&self) -> u64 {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .pop_front()
            .expect("clock fixture has a value")
    }

    fn sleep<'a>(
        &'a self,
        _duration: Duration,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(std::future::pending())
    }
}

struct FixedClock(u64);

impl Clock for FixedClock {
    fn now_unix_ms(&self) -> u64 {
        self.0
    }

    fn sleep<'a>(
        &'a self,
        _duration: Duration,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(std::future::pending())
    }
}

struct QueueEntropy {
    values: Mutex<VecDeque<[u8; 16]>>,
    calls: AtomicUsize,
}

impl UuidV7Entropy for QueueEntropy {
    fn random_bytes(&self) -> Result<[u8; 16], UuidV7EntropyError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Ok(self
            .values
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .pop_front()
            .expect("entropy fixture has a value"))
    }
}

struct FixedEntropy([u8; 16]);

impl UuidV7Entropy for FixedEntropy {
    fn random_bytes(&self) -> Result<[u8; 16], UuidV7EntropyError> {
        Ok(self.0)
    }
}

#[test]
fn uuid_v7_matches_rfc_layout_version_variant_and_lowercase_vector() {
    let mut first = [0_u8; 16];
    first[6..10].copy_from_slice(&0xffff_fffe_u32.to_be_bytes());
    first[10] = 1;
    first[11..].copy_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55]);
    let entropy = Arc::new(QueueEntropy {
        values: Mutex::new(VecDeque::from([first, [0; 16], [0; 16]])),
        calls: AtomicUsize::new(0),
    });
    let generator = UuidV7Generator::new(Arc::new(FixedClock(0x0123_4567_89ab)), entropy.clone());
    let first = generator.generate().expect("first UUID");
    assert_eq!(first, "01234567-89ab-7fff-bfff-f91122334455");
    assert!(first.bytes().all(|byte| !byte.is_ascii_uppercase()));
    assert_eq!(entropy.calls.load(Ordering::Acquire), 1);
}

#[test]
fn same_millisecond_sequence_rollover_advances_synthetic_timestamp() {
    let mut first = [0_u8; 16];
    first[6..10].copy_from_slice(&0xffff_fffe_u32.to_be_bytes());
    first[10] = 1;
    first[11..].copy_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55]);
    let entropy = Arc::new(QueueEntropy {
        values: Mutex::new(VecDeque::from([first, [0; 16], [0; 16]])),
        calls: AtomicUsize::new(0),
    });
    let generator = UuidV7Generator::new(Arc::new(FixedClock(0x0123_4567_89ab)), entropy.clone());
    let first = generator.generate().expect("first");
    let second = generator.generate().expect("second");
    let third = generator.generate().expect("third");
    assert_eq!(first, "01234567-89ab-7fff-bfff-f91122334455");
    assert_eq!(second, "01234567-89ab-7fff-bfff-fc0000000000");
    assert_eq!(third, "01234567-89ac-7000-8000-000000000000");
    assert!(first < second && second < third);
    assert_eq!(entropy.calls.load(Ordering::Acquire), 3);
}

#[test]
fn clock_rollback_preserves_lexicographic_monotonicity() {
    let generator = UuidV7Generator::new(
        Arc::new(QueueClock(Mutex::new(VecDeque::from([100, 99, 98])))),
        Arc::new(FixedEntropy([0; 16])),
    );
    let values = [
        generator.generate().expect("first"),
        generator.generate().expect("second"),
        generator.generate().expect("third"),
    ];
    assert!(values[0] < values[1] && values[1] < values[2]);
    assert!(
        values
            .iter()
            .all(|value| value.starts_with("00000000-0064-"))
    );
}

#[derive(Debug)]
struct SentinelError;

impl fmt::Display for SentinelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UUID_SECRET_SENTINEL")
    }
}

impl Error for SentinelError {}

struct FailingEntropy;

impl UuidV7Entropy for FailingEntropy {
    fn random_bytes(&self) -> Result<[u8; 16], UuidV7EntropyError> {
        Err(UuidV7EntropyError::with_source(SentinelError))
    }
}

#[test]
fn instances_are_explicit_and_concurrent_generation_is_unique_and_bounded() {
    let first = UuidV7Generator::new(Arc::new(FixedClock(100)), Arc::new(FixedEntropy([0; 16])));
    let second = UuidV7Generator::new(Arc::new(FixedClock(100)), Arc::new(FixedEntropy([0; 16])));
    assert_eq!(
        first.generate().expect("first instance"),
        second.generate().expect("second instance")
    );

    let concurrent = Arc::new(UuidV7Generator::new(
        Arc::new(FixedClock(200)),
        Arc::new(FixedEntropy([0; 16])),
    ));
    let threads = (0..32)
        .map(|_| {
            let generator = Arc::clone(&concurrent);
            thread::spawn(move || generator.generate().expect("UUID"))
        })
        .collect::<Vec<_>>();
    let values = threads
        .into_iter()
        .map(|thread| thread.join().expect("thread"))
        .collect::<BTreeSet<_>>();
    assert_eq!(values.len(), 32);

    let entropy_error = UuidV7Generator::new(Arc::new(FixedClock(1)), Arc::new(FailingEntropy))
        .generate()
        .expect_err("entropy fails");
    assert_eq!(entropy_error.code(), UuidV7ErrorCode::Entropy);
    assert!(!format!("{entropy_error:?} {entropy_error}").contains("UUID_SECRET_SENTINEL"));
    let entropy_source = Error::source(&entropy_error).expect("entropy error is retained");
    assert_eq!(
        entropy_source
            .source()
            .expect("backend source is retained")
            .to_string(),
        "UUID_SECRET_SENTINEL"
    );

    let entropy = Arc::new(QueueEntropy {
        values: Mutex::new(VecDeque::from([[1; 16], [0; 16]])),
        calls: AtomicUsize::new(0),
    });
    let range = UuidV7Generator::new(
        Arc::new(QueueClock(Mutex::new(VecDeque::from([1_u64 << 48, 1])))),
        entropy,
    );
    let error = range.generate().expect_err("timestamp out of range");
    assert_eq!(error.code(), UuidV7ErrorCode::TimestampOutOfRange);
    assert!(
        range
            .generate()
            .expect("state stayed empty")
            .starts_with("00000000-0001-7000-8000-")
    );
}
