use std::cell::{Cell, RefCell};
use std::fmt::Debug;
use std::rc::Rc;

use pi_ai_core::{
    AssistantMessage, Message, MessageTransformError, Model, SafeUnsigned, ToolCallId,
    ToolCallIdNormalizer, transform_messages,
};

const INVENTORY: &str =
    include_str!("../../../conformance/ai-domain/message-transform-inventory.json");
const CORE_TESTS: &str = include_str!("message_transform.rs");
const OBJECT_TESTS: &str = include_str!("message_transform_object_safety.rs");
const FACADE_TESTS: &str = include_str!("../../pi-ai/tests/message_transform_facade.rs");
type InventoryRow = (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
);
const EXPECTED_ROWS: [InventoryRow; 31] = [
    (
        "transform.lax.null-missing-content",
        "packages/ai/test/lax-message-content.test.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:31",
        "lax_decoder_repairs_null_and_missing_content_without_mutation",
        "boundary",
        "user assistant and tool-result missing or null content repair before strict decoding without input mutation",
    ),
    (
        "transform.images.capable-pass-through",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:35",
        "image_capable_model_preserves_input_blocks",
        "positive",
        "an image-capable target preserves user and tool-result image blocks",
    ),
    (
        "transform.images.user-placeholder",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:41",
        "text_only_model_uses_user_image_placeholder",
        "positive",
        "a text-only target replaces user images with the exact user placeholder",
    ),
    (
        "transform.images.tool-placeholder",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:48",
        "text_only_model_uses_tool_image_placeholder",
        "positive",
        "a text-only target replaces tool-result images with the distinct exact tool placeholder",
    ),
    (
        "transform.images.adjacent-collapse",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:15",
        "adjacent_unsupported_images_collapse_to_one_placeholder",
        "positive",
        "consecutive unsupported source images emit one placeholder",
    ),
    (
        "transform.images.existing-placeholder-collapse",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:29",
        "existing_placeholder_before_image_suppresses_duplicate",
        "boundary",
        "a preceding exact placeholder suppresses insertion while a same-text block following an image remains",
    ),
    (
        "transform.thinking.same-redacted-preserved",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:101",
        "same_model_preserves_redacted_thinking",
        "positive",
        "same-model redacted thinking remains byte-for-byte replayable",
    ),
    (
        "transform.thinking.foreign-redacted-dropped",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:101",
        "foreign_model_drops_redacted_thinking",
        "negative",
        "foreign redacted thinking is removed",
    ),
    (
        "transform.thinking.same-signed-empty-preserved",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:107",
        "same_model_preserves_signed_empty_thinking",
        "positive",
        "same-model empty or blank thinking with a nonempty signature remains",
    ),
    (
        "transform.thinking.unsigned-empty-dropped",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:110",
        "unsigned_empty_thinking_is_dropped",
        "negative",
        "empty or blank unsigned thinking is removed",
    ),
    (
        "transform.thinking.same-nonempty-preserved",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:112",
        "same_model_preserves_nonempty_thinking",
        "positive",
        "same-model nonempty thinking and metadata remain",
    ),
    (
        "transform.thinking.foreign-nonempty-to-text",
        "packages/ai/test/transform-messages-copilot-openai-to-anthropic.test.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:50",
        "foreign_nonempty_thinking_becomes_unsigned_text",
        "boundary",
        "foreign nonempty thinking becomes plain unsigned text",
    ),
    (
        "transform.text.same-signature-preserved",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:119",
        "same_model_preserves_text_signature",
        "positive",
        "same-model text signatures remain",
    ),
    (
        "transform.text.foreign-signature-removed",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:119",
        "foreign_model_removes_text_signature",
        "negative",
        "foreign text retains its text and loses its signature",
    ),
    (
        "transform.tool.same-thought-signature-preserved",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:127",
        "same_model_preserves_tool_thought_signature",
        "positive",
        "same-model tool calls preserve thought signatures",
    ),
    (
        "transform.tool.foreign-thought-signature-removed",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:131",
        "foreign_model_removes_only_tool_thought_signature",
        "negative",
        "foreign calls lose only a nonempty thought signature before id normalization while an empty signature follows JavaScript falsy preservation",
    ),
    (
        "transform.tool-id.same-model-bypass",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:95",
        "same_model_bypasses_tool_call_id_normalizer",
        "boundary",
        "an exact provider API and model match invokes no normalizer",
    ),
    (
        "transform.tool-id.foreign-normalized",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:136",
        "foreign_tool_calls_receive_target_and_source_normalization",
        "positive",
        "foreign tool calls normalize in content order with the complete source assistant and target model",
    ),
    (
        "transform.tool-id.following-result-remapped",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:83",
        "following_tool_results_use_normalized_ids",
        "positive",
        "a later matching tool result uses the changed target id",
    ),
    (
        "transform.tool-id.preceding-result-unchanged",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:83",
        "tool_results_preceding_their_calls_remain_unchanged",
        "boundary",
        "a tool result preceding its call is never retroactively rewritten",
    ),
    (
        "transform.tool-id.collision-rejected",
        "upstream-map.toml:861",
        "normalization_collision_is_rejected_without_partial_output",
        "negative",
        "distinct source owners of one final id fail atomically with bounded redacted diagnostics while repeated identical sources remain legal",
    ),
    (
        "transform.assistant.error-dropped",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:189",
        "errored_assistant_turn_is_dropped",
        "negative",
        "an error assistant is removed after its first-pass id mapping remains available to later results",
    ),
    (
        "transform.assistant.aborted-dropped",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:189",
        "aborted_assistant_turn_is_dropped",
        "negative",
        "an aborted assistant is removed without disturbing surrounding messages",
    ),
    (
        "transform.orphan.assistant-boundary",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:185",
        "assistant_boundary_repairs_previous_orphans",
        "positive",
        "an assistant boundary flushes missing results from the preceding assistant before itself",
    ),
    (
        "transform.orphan.user-boundary",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:210",
        "user_boundary_repairs_previous_orphans",
        "positive",
        "a user boundary flushes missing results before the user message",
    ),
    (
        "transform.orphan.end-boundary",
        "packages/ai/test/transform-messages-copilot-openai-to-anthropic.test.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:136",
        "end_boundary_repairs_trailing_orphans",
        "positive",
        "transcript end flushes trailing missing results",
    ),
    (
        "transform.orphan.partial-result-source-order",
        "packages/ai/test/transform-messages-copilot-openai-to-anthropic.test.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:162",
        "partial_results_synthesize_only_missing_calls_in_source_order",
        "positive",
        "only missing results synthesize and those synthetic results preserve source call order",
    ),
    (
        "transform.orphan.complete-results-no-synthesis",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:163",
        "complete_tool_results_do_not_synthesize_failures",
        "negative",
        "a complete result set adds no synthetic failures",
    ),
    (
        "transform.orphan.unrelated-result-preserved",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:207",
        "unrelated_tool_result_is_preserved_and_does_not_settle_pending_call",
        "boundary",
        "an unrelated result remains and does not settle the pending source call",
    ),
    (
        "transform.orphan.failed-assistant-boundary",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:185",
        "failed_assistant_boundary_repairs_previous_but_not_failed_calls",
        "boundary",
        "a failed assistant flushes prior pending calls but contributes no new pending calls",
    ),
    (
        "transform.orphan.injected-timestamp",
        "packages/ai/src/api/transform-messages.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185:163",
        "synthetic_results_use_injected_timestamp_and_exact_shape",
        "boundary",
        "every synthetic result has the exact fixed shape and caller-injected timestamp",
    ),
];

struct LocalNormalizer<'a> {
    prefix: &'a str,
    calls: Rc<Cell<usize>>,
    seen: Rc<RefCell<Vec<String>>>,
}

impl ToolCallIdNormalizer for LocalNormalizer<'_> {
    fn normalize_tool_call_id(
        &mut self,
        source_id: &ToolCallId,
        target_model: &Model,
        source_message: &AssistantMessage,
    ) -> ToolCallId {
        self.calls.set(self.calls.get() + 1);
        self.seen.borrow_mut().push(format!(
            "{}:{}:{}",
            source_id.as_str(),
            target_model.id.as_str(),
            source_message.model.as_str()
        ));
        ToolCallId::new(format!("{}{}", self.prefix, source_id.as_str()))
            .expect("prefixed fixture id is nonempty")
    }
}

fn accepts_normalizer(_: &mut dyn ToolCallIdNormalizer) {}

fn assert_error_contract<T>()
where
    T: Copy + Clone + Debug + Eq + std::error::Error + Send + Sync + 'static,
{
}

#[test]
fn message_transform_signature_and_policy_are_borrowed_and_object_safe() {
    type TransformFn =
        for<'messages, 'model, 'policy> fn(
            &'messages [Message],
            &'model Model,
            SafeUnsigned,
            Option<&'policy mut dyn ToolCallIdNormalizer>,
        ) -> Result<Vec<Message>, MessageTransformError>;

    let _transform: TransformFn = transform_messages;
    assert_error_contract::<MessageTransformError>();
    let error = MessageTransformError::NormalizedToolCallIdCollision;
    assert_eq!(format!("{error:?}"), "NormalizedToolCallIdCollision");
    assert_eq!(error.to_string(), "normalized tool-call identity collision");
    assert!(std::error::Error::source(&error).is_none());

    let stack_prefix = String::from("local-");
    let calls = Rc::new(Cell::new(0));
    let seen = Rc::new(RefCell::new(Vec::new()));
    let mut normalizer = LocalNormalizer {
        prefix: &stack_prefix,
        calls: Rc::clone(&calls),
        seen: Rc::clone(&seen),
    };
    accepts_normalizer(&mut normalizer);
    let source_id = ToolCallId::new("source").expect("fixture id");
    let model: Model = serde_json::from_value(serde_json::json!({
        "id":"target","name":"Target","api":"api","provider":"provider",
        "baseUrl":"https://example.invalid","reasoning":false,"input":["text"],
        "cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},
        "contextWindow":1,"maxTokens":1
    }))
    .expect("fixture model");
    let source: AssistantMessage = serde_json::from_value(serde_json::json!({
        "role":"assistant","content":[],"api":"source-api","provider":"source-provider",
        "model":"source-model","usage":{"input":0,"output":0,"cacheRead":0,
        "cacheWrite":0,"totalTokens":0,"cost":{"input":0,"output":0,"cacheRead":0,
        "cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":1
    }))
    .expect("fixture assistant");
    assert_eq!(
        normalizer
            .normalize_tool_call_id(&source_id, &model, &source)
            .as_str(),
        "local-source"
    );
    assert_eq!(calls.get(), 1);
    assert_eq!(
        seen.borrow().as_slice(),
        ["source:target:source-model".to_owned()]
    );
}

#[test]
fn message_transform_inventory_has_exact_rows_anchors_and_live_bindings() {
    let document: serde_json::Value = serde_json::from_str(INVENTORY).expect("valid inventory");
    assert_eq!(
        document
            .as_object()
            .expect("inventory object")
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>(),
        ["expectedRows", "rows", "suite"].into_iter().collect()
    );
    assert_eq!(document["suite"], "message-transform");
    assert_eq!(document["expectedRows"], 31);
    let rows = document["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 31);

    let exact_keys: std::collections::BTreeSet<&str> =
        ["expected", "id", "owner", "polarity", "source", "test"]
            .into_iter()
            .collect();
    for (row, expected) in rows.iter().zip(EXPECTED_ROWS) {
        let object = row.as_object().expect("row object");
        assert_eq!(
            object
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>(),
            exact_keys
        );
        let (id, source, test, polarity, observation) = expected;
        assert_eq!(row["id"], id);
        assert_eq!(row["source"], source);
        assert_eq!(row["test"], test);
        assert_eq!(row["polarity"], polarity);
        assert_eq!(row["owner"], "S03-message-transform");
        assert_eq!(row["expected"], observation);
        assert!(matches!(
            polarity,
            "positive" | "negative" | "boundary" | "race"
        ));
        let declaration = format!("fn {test}(");
        assert!(
            [CORE_TESTS, OBJECT_TESTS, FACADE_TESTS]
                .iter()
                .any(|source| source.contains(&declaration)),
            "missing test binding {test}"
        );
    }
}
