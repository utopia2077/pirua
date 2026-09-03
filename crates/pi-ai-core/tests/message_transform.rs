use std::collections::{BTreeMap, VecDeque};

use pi_ai_core::{
    ApiId, AssistantContent, AssistantMessage, AssistantRole, ImageContent, ImageContentType,
    InputContent, JsonObject, JsonValue, Message, MessageTransformError, Model, ModelId, Presence,
    ProviderId, SafeUnsigned, StopReason, TextContent, TextContentType, ThinkingContent,
    ThinkingContentType, ToolCall, ToolCallId, ToolCallIdNormalizer, ToolCallType, ToolName,
    ToolResultMessage, ToolResultRole, Usage, UserContent, UserMessage, UserRole,
    transform_messages,
};

const USER_PLACEHOLDER: &str = "(image omitted: model does not support images)";
const TOOL_PLACEHOLDER: &str = "(tool image omitted: model does not support images)";

fn safe(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).expect("fixture value is safe")
}

fn model(images: bool) -> Model {
    serde_json::from_value(serde_json::json!({
        "id": "target-model",
        "name": "Target Model",
        "api": "openai-completions",
        "provider": "target-provider",
        "baseUrl": "https://example.invalid/v1",
        "reasoning": true,
        "input": if images { serde_json::json!(["text", "image"]) } else { serde_json::json!(["text"]) },
        "thinkingLevelMap": { "minimal": "low", "max": null },
        "cost": {
            "input": 1,
            "output": 2,
            "cacheRead": 0.5,
            "cacheWrite": 1.5,
            "tiers": [{
                "input": 3,
                "output": 4,
                "cacheRead": 2.5,
                "cacheWrite": 3.5,
                "inputTokensAbove": 1000
            }]
        },
        "contextWindow": 128000,
        "maxTokens": 16000,
        "samplingParams": { "temperature": 0.25, "seed": 7 },
        "headers": {},
        "compat": { "supportsStore": true, "supportsStrictMode": false }
    }))
    .expect("fixture model is valid")
}

fn text(value: &str) -> InputContent {
    InputContent::Text(TextContent {
        content_type: TextContentType::Text,
        text: value.to_owned(),
        text_signature: None,
    })
}

fn signed_text(value: &str, signature: &str) -> AssistantContent {
    AssistantContent::Text(TextContent {
        content_type: TextContentType::Text,
        text: value.to_owned(),
        text_signature: Some(signature.to_owned()),
    })
}

fn image(value: &str) -> InputContent {
    InputContent::Image(ImageContent {
        content_type: ImageContentType::Image,
        data: value.to_owned(),
        mime_type: "image/png".to_owned(),
    })
}

fn thinking(value: &str, signature: Option<&str>, redacted: Option<bool>) -> AssistantContent {
    AssistantContent::Thinking(ThinkingContent {
        content_type: ThinkingContentType::Thinking,
        thinking: value.to_owned(),
        thinking_signature: signature.map(str::to_owned),
        redacted,
    })
}

fn tool_call(id: &str, name: &str, thought_signature: Option<&str>) -> AssistantContent {
    AssistantContent::ToolCall(ToolCall {
        content_type: ToolCallType::ToolCall,
        id: ToolCallId::new(id).expect("fixture id is nonempty"),
        name: ToolName::new(name).expect("fixture name is nonempty"),
        arguments: JsonObject::default(),
        thought_signature: thought_signature.map(str::to_owned),
        namespace: Some("fixture-namespace".to_owned()),
    })
}

fn user_blocks(content: Vec<InputContent>) -> Message {
    Message::User(UserMessage {
        role: UserRole::User,
        content: UserContent::Blocks(content),
        timestamp: safe(1),
    })
}

fn user_text(value: &str) -> Message {
    Message::User(UserMessage {
        role: UserRole::User,
        content: UserContent::Text(value.to_owned()),
        timestamp: safe(1),
    })
}

fn assistant(same_model: bool, stop_reason: StopReason, content: Vec<AssistantContent>) -> Message {
    let target = model(true);
    assistant_with_identity(
        if same_model {
            target.provider.as_str()
        } else {
            "source-provider"
        },
        if same_model {
            target.api.as_str()
        } else {
            "source-api"
        },
        if same_model {
            target.id.as_str()
        } else {
            "source-model"
        },
        stop_reason,
        content,
    )
}

fn assistant_with_identity(
    provider: &str,
    api: &str,
    model: &str,
    stop_reason: StopReason,
    content: Vec<AssistantContent>,
) -> Message {
    Message::Assistant(Box::new(AssistantMessage {
        role: AssistantRole::Assistant,
        content,
        api: ApiId::new(api).expect("fixture API"),
        provider: ProviderId::new(provider).expect("fixture provider"),
        model: ModelId::new(model).expect("fixture model"),
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: Usage::zero(),
        stop_reason,
        deferred: None,
        error_message: matches!(stop_reason, StopReason::Error | StopReason::Aborted)
            .then(|| "sanitized failure".to_owned()),
        raw_stop_reason: None,
        end_turn: None,
        timestamp: safe(2),
    }))
}

fn tool_result(id: &str, name: &str, content: Vec<InputContent>, is_error: bool) -> Message {
    Message::ToolResult(Box::new(ToolResultMessage::<JsonValue> {
        role: ToolResultRole::ToolResult,
        tool_call_id: ToolCallId::new(id).expect("fixture id is nonempty"),
        tool_name: ToolName::new(name).expect("fixture name is nonempty"),
        content,
        details: Presence::Missing,
        usage: None,
        added_tool_names: None,
        is_error,
        timestamp: safe(3),
    }))
}

fn transform(messages: &[Message], target: &Model) -> Vec<Message> {
    transform_messages(messages, target, safe(777), None).expect("transform succeeds")
}

fn assistant_content(message: &Message) -> &[AssistantContent] {
    let Message::Assistant(message) = message else {
        panic!("expected assistant message")
    };
    &message.content
}

fn tool_result_message(message: &Message) -> &ToolResultMessage {
    let Message::ToolResult(message) = message else {
        panic!("expected tool-result message")
    };
    message
}

#[derive(Default)]
struct RecordingNormalizer {
    calls: Vec<(ToolCallId, Model, AssistantMessage)>,
    mappings: BTreeMap<String, String>,
}

impl RecordingNormalizer {
    fn mapping(entries: &[(&str, &str)]) -> Self {
        Self {
            calls: Vec::new(),
            mappings: entries
                .iter()
                .map(|(source, target)| ((*source).to_owned(), (*target).to_owned()))
                .collect(),
        }
    }
}

impl ToolCallIdNormalizer for RecordingNormalizer {
    fn normalize_tool_call_id(
        &mut self,
        source_id: &ToolCallId,
        target_model: &Model,
        source_message: &AssistantMessage,
    ) -> ToolCallId {
        self.calls.push((
            source_id.clone(),
            target_model.clone(),
            source_message.clone(),
        ));
        ToolCallId::new(
            self.mappings
                .get(source_id.as_str())
                .map_or(source_id.as_str(), String::as_str),
        )
        .expect("fixture mapping is nonempty")
    }
}

struct ScriptedNormalizer {
    outputs: VecDeque<ToolCallId>,
}

impl ScriptedNormalizer {
    fn new(outputs: &[&str]) -> Self {
        Self {
            outputs: outputs
                .iter()
                .map(|value| ToolCallId::new(*value).expect("scripted id is nonempty"))
                .collect(),
        }
    }
}

impl ToolCallIdNormalizer for ScriptedNormalizer {
    fn normalize_tool_call_id(
        &mut self,
        _source_id: &ToolCallId,
        _target_model: &Model,
        _source_message: &AssistantMessage,
    ) -> ToolCallId {
        self.outputs
            .pop_front()
            .expect("script supplies one output per call")
    }
}

#[test]
fn image_capable_model_preserves_input_blocks() {
    let messages = vec![
        user_blocks(vec![text("before"), image("user-image")]),
        tool_result("call", "tool", vec![image("tool-image")], false),
    ];
    assert_eq!(transform(&messages, &model(true)), messages);
}

#[test]
fn text_only_model_uses_user_image_placeholder() {
    let output = transform(
        &[user_blocks(vec![text("before"), image("secret-image")])],
        &model(false),
    );
    let Message::User(message) = &output[0] else {
        panic!("expected user message")
    };
    assert_eq!(
        message.content,
        UserContent::Blocks(vec![text("before"), text(USER_PLACEHOLDER)])
    );
}

#[test]
fn text_only_model_uses_tool_image_placeholder() {
    let output = transform(
        &[tool_result(
            "call",
            "tool",
            vec![text("before"), image("secret-image")],
            false,
        )],
        &model(false),
    );
    assert_eq!(
        tool_result_message(&output[0]).content,
        vec![text("before"), text(TOOL_PLACEHOLDER)]
    );
}

#[test]
fn adjacent_unsupported_images_collapse_to_one_placeholder() {
    let output = transform(
        &[user_blocks(vec![
            image("one"),
            image("two"),
            image("three"),
        ])],
        &model(false),
    );
    let Message::User(message) = &output[0] else {
        panic!("expected user message")
    };
    assert_eq!(
        message.content,
        UserContent::Blocks(vec![text(USER_PLACEHOLDER)])
    );
}

#[test]
fn existing_placeholder_before_image_suppresses_duplicate() {
    let output = transform(
        &[user_blocks(vec![
            text(USER_PLACEHOLDER),
            image("suppressed"),
            text("separator"),
            image("inserted"),
            text(USER_PLACEHOLDER),
        ])],
        &model(false),
    );
    let Message::User(message) = &output[0] else {
        panic!("expected user message")
    };
    assert_eq!(
        message.content,
        UserContent::Blocks(vec![
            text(USER_PLACEHOLDER),
            text("separator"),
            text(USER_PLACEHOLDER),
            text(USER_PLACEHOLDER),
        ])
    );
}

#[test]
fn same_model_preserves_redacted_thinking() {
    let block = thinking("opaque", Some("signature"), Some(true));
    let output = transform(
        &[assistant(true, StopReason::Stop, vec![block.clone()])],
        &model(true),
    );
    assert_eq!(assistant_content(&output[0]), &[block]);
}

#[test]
fn foreign_model_drops_redacted_thinking() {
    let output = transform(
        &[assistant(
            false,
            StopReason::Stop,
            vec![thinking("opaque", Some("signature"), Some(true))],
        )],
        &model(true),
    );
    assert!(assistant_content(&output[0]).is_empty());
}

#[test]
fn same_model_preserves_signed_empty_thinking() {
    for value in ["", " \t\n"] {
        let block = thinking(value, Some("signature"), None);
        let output = transform(
            &[assistant(true, StopReason::Stop, vec![block.clone()])],
            &model(true),
        );
        assert_eq!(assistant_content(&output[0]), &[block]);
    }
}

#[test]
fn unsigned_empty_thinking_is_dropped() {
    const ECMASCRIPT_WHITESPACE: &str = "\u{0009}\u{000a}\u{000b}\u{000c}\u{000d}\u{0020}\u{00a0}\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\u{2004}\u{2005}\u{2006}\u{2007}\u{2008}\u{2009}\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}\u{feff}";
    for (value, same_model, signature) in [
        (ECMASCRIPT_WHITESPACE, true, None),
        (ECMASCRIPT_WHITESPACE, true, Some("")),
        (ECMASCRIPT_WHITESPACE, false, Some("sig")),
    ] {
        let output = transform(
            &[assistant(
                same_model,
                StopReason::Stop,
                vec![thinking(value, signature, None)],
            )],
            &model(true),
        );
        assert!(assistant_content(&output[0]).is_empty());
    }

    for non_whitespace in ["\u{0085}", "\u{180e}"] {
        let block = thinking(non_whitespace, None, None);
        let output = transform(
            &[assistant(true, StopReason::Stop, vec![block.clone()])],
            &model(true),
        );
        assert_eq!(assistant_content(&output[0]), &[block]);
    }
}

#[test]
fn same_model_preserves_nonempty_thinking() {
    let block = thinking("reasoning", None, Some(false));
    let output = transform(
        &[assistant(true, StopReason::Stop, vec![block.clone()])],
        &model(true),
    );
    assert_eq!(assistant_content(&output[0]), &[block]);
}

#[test]
fn foreign_nonempty_thinking_becomes_unsigned_text() {
    let output = transform(
        &[assistant(
            false,
            StopReason::Stop,
            vec![thinking("reasoning", Some("secret-signature"), Some(false))],
        )],
        &model(true),
    );
    assert_eq!(
        assistant_content(&output[0]),
        &[AssistantContent::Text(TextContent {
            content_type: TextContentType::Text,
            text: "reasoning".to_owned(),
            text_signature: None,
        })]
    );
}

#[test]
fn same_model_preserves_text_signature() {
    let block = signed_text("answer", "secret-signature");
    let output = transform(
        &[assistant(true, StopReason::Stop, vec![block.clone()])],
        &model(true),
    );
    assert_eq!(assistant_content(&output[0]), &[block]);
}

#[test]
fn foreign_model_removes_text_signature() {
    let output = transform(
        &[assistant(
            false,
            StopReason::Stop,
            vec![signed_text("answer", "secret-signature")],
        )],
        &model(true),
    );
    assert_eq!(
        assistant_content(&output[0]),
        &[AssistantContent::Text(match text("answer") {
            InputContent::Text(text) => text,
            InputContent::Image(_) => unreachable!(),
        })]
    );
}

#[test]
fn same_model_preserves_tool_thought_signature() {
    let block = tool_call("call", "tool", Some("secret-signature"));
    let output = transform(
        &[
            assistant(true, StopReason::ToolUse, vec![block.clone()]),
            tool_result("call", "tool", vec![], false),
        ],
        &model(true),
    );
    assert_eq!(assistant_content(&output[0]), &[block]);
}

#[test]
fn foreign_model_removes_only_tool_thought_signature() {
    let output = transform(
        &[
            assistant(
                false,
                StopReason::ToolUse,
                vec![
                    tool_call("nonempty", "tool", Some("secret-signature")),
                    tool_call("empty", "tool", Some("")),
                ],
            ),
            tool_result("nonempty", "tool", vec![], false),
            tool_result("empty", "tool", vec![], false),
        ],
        &model(true),
    );
    let AssistantContent::ToolCall(nonempty) = &assistant_content(&output[0])[0] else {
        panic!("expected tool call")
    };
    let AssistantContent::ToolCall(empty) = &assistant_content(&output[0])[1] else {
        panic!("expected tool call")
    };
    assert_eq!(nonempty.id.as_str(), "nonempty");
    assert_eq!(nonempty.name.as_str(), "tool");
    assert_eq!(nonempty.namespace.as_deref(), Some("fixture-namespace"));
    assert_eq!(nonempty.arguments, JsonObject::default());
    assert_eq!(nonempty.thought_signature, None);
    assert_eq!(empty.thought_signature.as_deref(), Some(""));
}

#[test]
fn same_model_bypasses_tool_call_id_normalizer() {
    let target = model(true);
    let messages = [
        assistant(
            true,
            StopReason::ToolUse,
            vec![tool_call("call", "tool", None)],
        ),
        tool_result("call", "tool", vec![], false),
    ];
    let mut normalizer = RecordingNormalizer::mapping(&[("call", "changed")]);
    let output = transform_messages(&messages, &target, safe(777), Some(&mut normalizer))
        .expect("same-model transform succeeds");
    assert!(normalizer.calls.is_empty());
    assert_eq!(
        tool_result_message(&output[1]).tool_call_id.as_str(),
        "call"
    );

    for (provider, api, model_id) in [
        ("other-provider", target.api.as_str(), target.id.as_str()),
        (target.provider.as_str(), "other-api", target.id.as_str()),
        (target.provider.as_str(), target.api.as_str(), "other-model"),
    ] {
        let messages = [
            assistant_with_identity(
                provider,
                api,
                model_id,
                StopReason::ToolUse,
                vec![tool_call("call", "tool", None)],
            ),
            tool_result("call", "tool", vec![], false),
        ];
        let mut normalizer = RecordingNormalizer::mapping(&[("call", "changed")]);
        let output = transform_messages(&messages, &target, safe(777), Some(&mut normalizer))
            .expect("one-field mismatch is foreign");
        assert_eq!(normalizer.calls.len(), 1);
        assert_eq!(
            tool_result_message(&output[1]).tool_call_id.as_str(),
            "changed"
        );
    }
}

#[test]
fn foreign_tool_calls_receive_target_and_source_normalization() {
    let target = model(true);
    let source = assistant(
        false,
        StopReason::ToolUse,
        vec![
            thinking("private reasoning", Some("thinking-signature"), None),
            signed_text("signed answer", "text-signature"),
            tool_call("one", "tool", Some("thought-signature")),
            tool_call("two", "tool", None),
        ],
    );
    let Message::Assistant(source_message) = &source else {
        panic!("expected assistant fixture")
    };
    let expected_source = (**source_message).clone();
    let messages = [
        source,
        tool_result("one", "tool", vec![], false),
        tool_result("two", "tool", vec![], false),
    ];
    let original_messages = messages.clone();
    let original_target = target.clone();
    let mut normalizer = RecordingNormalizer::mapping(&[("one", "n-one"), ("two", "n-two")]);
    let output = transform_messages(&messages, &target, safe(777), Some(&mut normalizer))
        .expect("foreign transform succeeds");
    assert_eq!(messages, original_messages);
    assert_eq!(target, original_target);
    assert_eq!(normalizer.calls.len(), 2);
    for (index, expected_id) in ["one", "two"].iter().enumerate() {
        assert_eq!(normalizer.calls[index].0.as_str(), *expected_id);
        assert_eq!(normalizer.calls[index].1, target);
        assert_eq!(normalizer.calls[index].2, expected_source);
    }
    assert_eq!(
        assistant_content(&output[0])
            .iter()
            .filter_map(|block| match block {
                AssistantContent::ToolCall(call) => Some(call.id.as_str()),
                AssistantContent::Text(_) | AssistantContent::Thinking(_) => None,
            })
            .collect::<Vec<_>>(),
        ["n-one", "n-two"]
    );
}

#[test]
fn following_tool_results_use_normalized_ids() {
    let messages = [
        assistant(
            false,
            StopReason::ToolUse,
            vec![tool_call("source", "tool", None)],
        ),
        tool_result("source", "tool", vec![], false),
    ];
    let mut normalizer = RecordingNormalizer::mapping(&[("source", "target")]);
    let output = transform_messages(&messages, &model(true), safe(777), Some(&mut normalizer))
        .expect("transform succeeds");
    assert_eq!(
        tool_result_message(&output[1]).tool_call_id.as_str(),
        "target"
    );
    assert_eq!(output.len(), 2);
}

#[test]
fn tool_results_preceding_their_calls_remain_unchanged() {
    let messages = [
        tool_result("source", "tool", vec![], false),
        assistant(
            false,
            StopReason::ToolUse,
            vec![tool_call("source", "tool", None)],
        ),
    ];
    let mut normalizer = RecordingNormalizer::mapping(&[("source", "target")]);
    let output = transform_messages(&messages, &model(true), safe(777), Some(&mut normalizer))
        .expect("transform succeeds");
    assert_eq!(
        tool_result_message(&output[0]).tool_call_id.as_str(),
        "source"
    );
    assert_eq!(
        tool_result_message(output.last().expect("synthetic result"))
            .tool_call_id
            .as_str(),
        "target"
    );
}

#[test]
fn normalization_collision_is_rejected_without_partial_output() {
    const SENTINEL: &str = "COLLISION_SECRET_7fd4";
    let cases = [
        (
            StopReason::ToolUse,
            ["changed-one", "changed-two"],
            [("changed-one", "final"), ("changed-two", "final")],
        ),
        (
            StopReason::ToolUse,
            ["final", "changed-after"],
            [("unused", "unused"), ("changed-after", "final")],
        ),
        (
            StopReason::ToolUse,
            [SENTINEL, "final"],
            [(SENTINEL, "final"), ("unused", "unused")],
        ),
        (
            StopReason::Error,
            ["failed-one", "failed-two"],
            [("failed-one", "final"), ("failed-two", "final")],
        ),
    ];

    for (stop_reason, source_ids, mappings) in cases {
        let messages = [assistant(
            false,
            stop_reason,
            vec![
                tool_call(source_ids[0], "tool", None),
                tool_call(source_ids[1], "tool", None),
            ],
        )];
        let mut normalizer = RecordingNormalizer::mapping(&mappings);
        let error = transform_messages(&messages, &model(true), safe(777), Some(&mut normalizer))
            .expect_err("distinct final-id owners must collide in every ordering");
        assert_eq!(error, MessageTransformError::NormalizedToolCallIdCollision);
        assert_eq!(normalizer.calls.len(), 2);
        assert!(!format!("{error:?} {error}").contains(SENTINEL));
    }

    let repeated_source = [
        assistant(
            false,
            StopReason::ToolUse,
            vec![tool_call("same", "tool", None)],
        ),
        tool_result("same", "tool", vec![], false),
        assistant(
            false,
            StopReason::ToolUse,
            vec![tool_call("same", "tool", None)],
        ),
        tool_result("same", "tool", vec![], false),
        assistant(
            false,
            StopReason::ToolUse,
            vec![tool_call("same", "tool", None)],
        ),
        tool_result("same", "tool", vec![], false),
    ];
    let mut normalizer = ScriptedNormalizer::new(&["dynamic-x", "dynamic-y", "same"]);
    let output = transform_messages(
        &repeated_source,
        &model(true),
        safe(777),
        Some(&mut normalizer),
    )
    .expect("repeated source ownership remains legal");
    assert_eq!(
        output
            .iter()
            .filter_map(|message| match message {
                Message::ToolResult(result) =>
                    Some((result.tool_call_id.as_str(), result.is_error,)),
                Message::User(_) | Message::Assistant(_) => None,
            })
            .collect::<Vec<_>>(),
        [
            ("dynamic-x", false),
            ("dynamic-y", false),
            ("dynamic-y", false),
            ("same", true),
        ]
    );
}

#[test]
fn errored_assistant_turn_is_dropped() {
    let messages = [
        assistant(
            false,
            StopReason::Error,
            vec![tool_call("source", "tool", None)],
        ),
        tool_result("source", "tool", vec![], false),
    ];
    let mut normalizer = RecordingNormalizer::mapping(&[("source", "target")]);
    let output = transform_messages(&messages, &model(true), safe(777), Some(&mut normalizer))
        .expect("transform succeeds");
    assert_eq!(output.len(), 1);
    assert_eq!(
        tool_result_message(&output[0]).tool_call_id.as_str(),
        "target"
    );
}

#[test]
fn aborted_assistant_turn_is_dropped() {
    let output = transform(
        &[
            user_text("before"),
            assistant(
                false,
                StopReason::Aborted,
                vec![signed_text("partial", "secret")],
            ),
            user_text("after"),
        ],
        &model(true),
    );
    assert_eq!(output, vec![user_text("before"), user_text("after")]);
}

#[test]
fn assistant_boundary_repairs_previous_orphans() {
    let output = transform(
        &[
            assistant(
                true,
                StopReason::ToolUse,
                vec![tool_call("first", "a", None)],
            ),
            assistant(true, StopReason::Stop, vec![signed_text("answer", "sig")]),
        ],
        &model(true),
    );
    assert_eq!(output.len(), 3);
    assert_eq!(
        tool_result_message(&output[1]).tool_call_id.as_str(),
        "first"
    );
    assert!(matches!(output[2], Message::Assistant(_)));
}

#[test]
fn user_boundary_repairs_previous_orphans() {
    let output = transform(
        &[
            assistant(
                true,
                StopReason::ToolUse,
                vec![tool_call("first", "a", None)],
            ),
            user_text("interrupt"),
        ],
        &model(true),
    );
    assert_eq!(output.len(), 3);
    assert!(matches!(output[2], Message::User(_)));
    assert_eq!(
        tool_result_message(&output[1]).tool_call_id.as_str(),
        "first"
    );
}

#[test]
fn end_boundary_repairs_trailing_orphans() {
    let output = transform(
        &[assistant(
            true,
            StopReason::ToolUse,
            vec![tool_call("last", "a", None)],
        )],
        &model(true),
    );
    assert_eq!(output.len(), 2);
    assert_eq!(
        tool_result_message(&output[1]).tool_call_id.as_str(),
        "last"
    );
}

#[test]
fn partial_results_synthesize_only_missing_calls_in_source_order() {
    let output = transform(
        &[
            assistant(
                true,
                StopReason::ToolUse,
                vec![
                    tool_call("one", "a", None),
                    tool_call("two", "b", None),
                    tool_call("three", "c", None),
                ],
            ),
            tool_result("two", "b", vec![text("done")], false),
        ],
        &model(true),
    );
    assert_eq!(
        output
            .iter()
            .skip(1)
            .map(|message| tool_result_message(message).tool_call_id.as_str())
            .collect::<Vec<_>>(),
        ["two", "one", "three"]
    );
}

#[test]
fn complete_tool_results_do_not_synthesize_failures() {
    let output = transform(
        &[
            assistant(
                true,
                StopReason::ToolUse,
                vec![tool_call("one", "a", None), tool_call("two", "b", None)],
            ),
            tool_result("one", "a", vec![], false),
            tool_result("two", "b", vec![], false),
        ],
        &model(true),
    );
    assert_eq!(output.len(), 3);
    assert!(
        output
            .iter()
            .skip(1)
            .all(|message| !tool_result_message(message).is_error)
    );

    let duplicate_without_result = transform(
        &[assistant(
            true,
            StopReason::ToolUse,
            vec![
                tool_call("duplicate", "a", None),
                tool_call("duplicate", "a", None),
            ],
        )],
        &model(true),
    );
    assert_eq!(duplicate_without_result.len(), 3);
    assert!(duplicate_without_result[1..].iter().all(|message| {
        let result = tool_result_message(message);
        result.is_error && result.tool_call_id.as_str() == "duplicate"
    }));

    let duplicate_with_one_result = transform(
        &[
            assistant(
                true,
                StopReason::ToolUse,
                vec![
                    tool_call("duplicate", "a", None),
                    tool_call("duplicate", "a", None),
                ],
            ),
            tool_result("duplicate", "a", vec![], false),
        ],
        &model(true),
    );
    assert_eq!(duplicate_with_one_result.len(), 2);
    assert!(!tool_result_message(&duplicate_with_one_result[1]).is_error);
}

#[test]
fn unrelated_tool_result_is_preserved_and_does_not_settle_pending_call() {
    let unrelated = tool_result("other", "other-tool", vec![text("value")], false);
    let output = transform(
        &[
            assistant(
                true,
                StopReason::ToolUse,
                vec![tool_call("pending", "a", None)],
            ),
            unrelated.clone(),
        ],
        &model(true),
    );
    assert_eq!(output[1], unrelated);
    assert_eq!(output.len(), 3);
    assert_eq!(
        tool_result_message(&output[2]).tool_call_id.as_str(),
        "pending"
    );
}

#[test]
fn failed_assistant_boundary_repairs_previous_but_not_failed_calls() {
    let output = transform(
        &[
            assistant(
                true,
                StopReason::ToolUse,
                vec![tool_call("previous", "a", None)],
            ),
            assistant(
                true,
                StopReason::Error,
                vec![tool_call("failed", "b", None)],
            ),
        ],
        &model(true),
    );
    assert_eq!(output.len(), 2);
    assert_eq!(
        tool_result_message(&output[1]).tool_call_id.as_str(),
        "previous"
    );
}

#[test]
fn synthetic_results_use_injected_timestamp_and_exact_shape() {
    let timestamp = safe(424_242);
    let output = transform_messages(
        &[assistant(
            true,
            StopReason::ToolUse,
            vec![tool_call("call", "tool", None)],
        )],
        &model(true),
        timestamp,
        None,
    )
    .expect("transform succeeds");
    let result = tool_result_message(&output[1]);
    assert_eq!(result.role, ToolResultRole::ToolResult);
    assert_eq!(result.tool_call_id.as_str(), "call");
    assert_eq!(result.tool_name.as_str(), "tool");
    assert_eq!(result.content, vec![text("No result provided")]);
    assert_eq!(result.details, Presence::Missing);
    assert_eq!(result.usage, None);
    assert_eq!(result.added_tool_names, None);
    assert!(result.is_error);
    assert_eq!(result.timestamp, timestamp);
}
