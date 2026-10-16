use std::collections::BTreeSet;
use std::error::Error;
use std::sync::Arc;

use pi_ai_core::{
    AssistantContent, Clock, ContentTextInput, ContextEstimateInput, ContextUsageEstimate,
    InputContent, JsonParseError, JsonParseErrorCode, JsonValue, Message, NumericError,
    SafeUnsigned, TextAndImageContentRef, Usage, UuidV7Entropy, UuidV7EntropyError, UuidV7Error,
    UuidV7ErrorCode, UuidV7Generator, calculate_context_tokens, content_text,
    content_text_with_separator, estimate_context_tokens, estimate_message_tokens,
    estimate_text_and_image_content_tokens, estimate_text_tokens, parse_json_with_repair,
    parse_streaming_json, repair_json, short_hash,
};

const INVENTORY: &str =
    include_str!("../../../conformance/ai-domain/portable-helpers-inventory.json");
const TESTS: &str = include_str!("portable_helpers.rs");
const EXPECTED_IDS: [&str; 23] = [
    "helper.context.usage-total-fallback",
    "helper.context.utf16-text-image",
    "helper.context.message-roles",
    "helper.context.no-checkpoint-prefix",
    "helper.context.checkpoint-trailing",
    "helper.context.invalid-checkpoint-matrix",
    "helper.context.newer-checkpoint",
    "helper.context.added-tools-after-checkpoint",
    "helper.json.repair-valid",
    "helper.json.repair-controls",
    "helper.json.repair-invalid-escapes",
    "helper.json.parse-repair-order",
    "helper.json.parse-error-redaction",
    "helper.json.streaming-fallback",
    "helper.json.streaming-partial",
    "helper.text.string-separator",
    "helper.text.block-filtering",
    "helper.hash.wrapping-base36",
    "helper.hash.utf16",
    "helper.uuid.rfc-layout",
    "helper.uuid.sequence-rollover",
    "helper.uuid.clock-rollback",
    "helper.uuid.instance-concurrency-error",
];

fn assert_value<T: Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static>() {}
fn assert_error<T: std::fmt::Debug + Error + Send + Sync + 'static>() {}

fn random_bytes(entropy: &dyn UuidV7Entropy) -> Result<[u8; 16], UuidV7EntropyError> {
    entropy.random_bytes()
}

fn input_variant(value: &[InputContent]) -> ContentTextInput<'_> {
    ContentTextInput::Input(value)
}

fn assistant_variant(value: &[AssistantContent]) -> ContentTextInput<'_> {
    ContentTextInput::Assistant(value)
}

#[test]
fn portable_helper_public_api_is_exact_and_object_safe() {
    let _usage: fn(&Usage) -> Result<SafeUnsigned, NumericError> = calculate_context_tokens;
    let _text_tokens: fn(&str) -> Result<SafeUnsigned, NumericError> = estimate_text_tokens;
    let _content_tokens: fn(TextAndImageContentRef<'_>) -> Result<SafeUnsigned, NumericError> =
        estimate_text_and_image_content_tokens;
    let _message_tokens: fn(&Message) -> Result<SafeUnsigned, NumericError> =
        estimate_message_tokens;
    let _context_tokens: fn(
        ContextEstimateInput<'_>,
    ) -> Result<ContextUsageEstimate, NumericError> = estimate_context_tokens;
    let _repair: fn(&str) -> String = repair_json;
    let _parse: fn(&str) -> Result<serde_json::Value, JsonParseError> =
        parse_json_with_repair::<serde_json::Value>;
    let _streaming: fn(Option<&str>) -> JsonValue = parse_streaming_json;
    let _content: fn(ContentTextInput<'_>) -> String = content_text;
    let _content_separator: fn(ContentTextInput<'_>, &str) -> String = content_text_with_separator;
    let _hash: fn(&str) -> String = short_hash;
    let _uuid_new: fn(Arc<dyn Clock>, Arc<dyn UuidV7Entropy>) -> UuidV7Generator =
        UuidV7Generator::new;
    let _uuid_generate: fn(&UuidV7Generator) -> Result<String, UuidV7Error> =
        UuidV7Generator::generate;
    let _uuid_code: fn(&UuidV7Error) -> UuidV7ErrorCode = UuidV7Error::code;

    fn accepts_entropy(_: &dyn UuidV7Entropy) {}
    let _entropy: fn(&dyn UuidV7Entropy) = accepts_entropy;
    let _random: fn(&dyn UuidV7Entropy) -> Result<[u8; 16], UuidV7EntropyError> = random_bytes;

    assert_value::<ContextUsageEstimate>();
    assert_error::<JsonParseError>();
    assert_error::<UuidV7EntropyError>();
    assert_error::<UuidV7Error>();
    fn assert_generator<T: std::fmt::Debug + Send + Sync + 'static>() {}
    assert_generator::<UuidV7Generator>();

    let error = parse_json_with_repair::<serde_json::Value>("PORTABLE_SECRET_SENTINEL")
        .expect_err("invalid JSON");
    assert_eq!(error.code(), JsonParseErrorCode::InvalidJson);
    assert!(Error::source(&error).is_none());
    assert!(!format!("{error:?} {error}").contains("PORTABLE_SECRET_SENTINEL"));

    let _input_variant: for<'a> fn(&'a [InputContent]) -> ContentTextInput<'a> = input_variant;
    let _assistant_variant: for<'a> fn(&'a [AssistantContent]) -> ContentTextInput<'a> =
        assistant_variant;
}

#[test]
fn portable_helpers_inventory_has_exact_rows_and_live_bindings() {
    let document: serde_json::Value = serde_json::from_str(INVENTORY).expect("valid inventory");
    assert_eq!(document["suite"], "portable-helpers");
    assert_eq!(document["expectedRows"], 23);
    let rows = document["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), EXPECTED_IDS.len());
    let keys = ["expected", "id", "owner", "polarity", "source", "test"]
        .into_iter()
        .collect::<BTreeSet<_>>();
    for (row, expected_id) in rows.iter().zip(EXPECTED_IDS) {
        let row = row.as_object().expect("row object");
        assert_eq!(
            row.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            keys
        );
        assert_eq!(row["id"], expected_id);
        assert_eq!(row["owner"], "S03-portable-helpers");
        assert!(matches!(
            row["polarity"].as_str(),
            Some("positive" | "negative" | "boundary" | "race")
        ));
        let source = row["source"].as_str().expect("source");
        assert!(
            source.contains("@853a80d26c90a14c1886f0ebb8ffaae133ca2185")
                || source.contains("docs/contracts/ai-domain/domain.md#")
        );
        let binding = row["test"].as_str().expect("binding");
        assert!(TESTS.contains(&format!("fn {binding}(")));
        assert!(!row["expected"].as_str().expect("expected").is_empty());
    }
}
