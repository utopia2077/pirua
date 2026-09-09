use pi_ai_core::{Presence, Tool};
use pi_ai_providers::api::{
    openai_responses_shared::{ConvertResponsesToolsOptions, convert_responses_tools},
    wire_json::stringify_wire_json,
};
use serde_json::Value;
#[test]
fn deferred_function_slots_match_complete_pinned_converter() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openai-responses-shared/deferred-order.json"
    ))
    .unwrap();
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 24);
    for case in fixture["cases"].as_array().unwrap() {
        let options = &case["options"];
        let strict = match options.get("strict") {
            None => Presence::Missing,
            Some(Value::Null) => Presence::Null,
            Some(value) => Presence::Value(value.as_bool().unwrap()),
        };
        let tool: Tool = serde_json::from_value(case["tool"].clone()).unwrap();
        let actual = convert_responses_tools(
            &[tool],
            &ConvertResponsesToolsOptions {
                strict,
                defer_loading: options["deferLoading"].as_bool(),
                supports_strict_mode: options["supportsStrictMode"].as_bool(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            stringify_wire_json(&serde_json::json!(actual)).unwrap(),
            case["expectedJson"].as_str().unwrap(),
            "{options}"
        );
    }
}
