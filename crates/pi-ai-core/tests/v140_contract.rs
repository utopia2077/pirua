use pi_ai_core::*;
use serde_json::{Value, json};
fn helpers() -> Value {
    serde_json::from_str::<Value>(include_str!(
        "../../../conformance/provider-traces/v1.4.0/wire-observations.json"
    ))
    .unwrap()["helpers"]
        .clone()
}
#[test]
fn explicit_null_identity_disables_inherited_attribution_and_roundtrips() {
    for case in helpers()["identities"].as_array().unwrap() {
        let base: ClientIdentity = serde_json::from_value(case["base"].clone()).unwrap();
        let request: ClientIdentity = serde_json::from_value(case["request"].clone()).unwrap();
        let merged = merge_client_identity(Some(&base), Some(&request));
        assert_eq!(serde_json::to_value(&merged).unwrap(), case["merged"]);
        assert_eq!(
            serde_json::to_value(get_identity_headers(
                "custom",
                "https://openrouter.ai/api/v1",
                Some(&merged)
            ))
            .unwrap(),
            case["headers"]
        );
    }
    let disabled: ClientIdentity = serde_json::from_value(json!({"attribution":null})).unwrap();
    assert_eq!(disabled.attribution, Presence::Null);
    assert_eq!(
        serde_json::to_value(disabled).unwrap(),
        json!({"attribution":null})
    );
    assert_eq!(
        serde_json::to_value(ClientIdentity::default()).unwrap(),
        json!({})
    );
    assert_eq!(
        format_user_agent(Some(&ClientIdentity {
            user_agent: Some(UserAgentIdentity::Mode(UserAgentMode::Short)),
            ..Default::default()
        })),
        "pi"
    );
    assert!(format_user_agent(None).starts_with("pi/1.4.0 ("));
}
#[test]
fn ordered_transcript_sections_and_tool_replacements_match_release() {
    let case = &helpers()["transcript"];
    let normalized = normalize_context(serde_json::from_value(case["input"].clone()).unwrap());
    assert_eq!(
        serde_json::to_value(get_current_system_message(&normalized.messages)).unwrap(),
        case["current"]
    );
    assert_eq!(
        serde_json::to_value(get_current_tools(&normalized.messages)).unwrap(),
        case["tools"]
    );
    assert_eq!(
        collapse_system_messages(normalized),
        serde_json::from_value::<TranscriptContext>(case["collapsed"].clone()).unwrap()
    );
    let context:Context=serde_json::from_value(json!({"systemPrompt":"head","tools":case["tools"],"messages":[{"role":"user","content":"hi","timestamp":1}]})).unwrap();
    let normalized = normalize_context(context);
    assert_eq!(normalized.messages.len(), 2);
    assert_eq!(get_current_system_prompt(&normalized.messages), "head");
    assert_eq!(
        serde_json::to_value(get_current_tools(&normalized.messages)).unwrap(),
        case["tools"]
    );
    let replaced:Vec<Tool>=serde_json::from_value(json!([{ "name":"late","description":"new","parameters":{"type":"object","properties":{}}}])).unwrap();
    let old = get_current_tools(&normalized.messages);
    let changes = get_tool_state_changes(&old, &replaced);
    assert_eq!(changes.tools_removed.len(), 1);
    assert_eq!(changes.tools_added, replaced);
}
#[test]
fn numeric_section_keys_and_duplicate_values_follow_object_property_order() {
    let helper = helpers();
    let case = &helper["sectionOrder"];
    let entries: Vec<(String, Option<String>)> =
        serde_json::from_value(case["entries"].clone()).unwrap();
    let mut message: SystemMessage = serde_json::from_value(case["message"].clone()).unwrap();
    message.sections = Some(SystemSections::new(entries));
    assert_eq!(serde_json::to_value(&message).unwrap(), case["message"]);
    assert_eq!(get_system_message_text(&message), case["prompt"]);
}
#[test]
fn malformed_finished_arguments_preserve_text_and_fail_validation() {
    let tool: Tool = serde_json::from_value(
        json!({"name":"echo","description":"Echo","parameters":{"type":"object","properties":{}}}),
    )
    .unwrap();
    for text in ["[]", "3", "null", r#"{"broken": !!!}"#] {
        let parsed = parse_tool_call_arguments(Some(text));
        assert_eq!(parsed.arguments, JsonObject::default());
        let diagnostic = parsed.arguments_error.unwrap();
        assert!(diagnostic.contains(text));
        let call:ToolCall=serde_json::from_value(json!({"type":"toolCall","id":"c","name":"echo","arguments":{},"argumentsError":diagnostic})).unwrap();
        assert!(matches!(
            validate_tool_arguments(&tool, &call),
            Err(ToolValidationError::MalformedArguments)
        ));
    }
    for text in [None, Some(""), Some("  "), Some("{\"value\":2}")] {
        assert!(parse_tool_call_arguments(text).arguments_error.is_none());
    }
}
#[test]
fn retry_classifier_uses_status_boundaries_and_subscription_limit_precedence() {
    let mut message:AssistantMessage=serde_json::from_value(json!({"role":"assistant","content":[],"api":"openai-responses","provider":"openai","model":"m","usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"error","timestamp":0})).unwrap();
    for (text, retry) in [
        ("status 520", true),
        ("request 503 failed", true),
        ("x503", false),
        ("5030", false),
        ("request-id-500A", false),
        ("subscription_sharing_usage_unavailable", true),
        ("subscription_sharing_usage_limit_exceeded: 429", false),
        (
            "subscription_sharing_workplace_individual_usage_limit_exceeded: 503",
            false,
        ),
    ] {
        message.error_message = Some(text.into());
        assert_eq!(is_retryable_assistant_error(&message), retry, "{text}");
    }
}
