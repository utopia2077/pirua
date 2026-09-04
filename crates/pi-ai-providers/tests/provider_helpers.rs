use std::borrow::Cow;
use std::collections::BTreeMap;

use pi_ai_core::{JsonObject, JsonSchema, JsonValue, Message, Tool};
use pi_ai_providers::api::constrained_sampling::{
    ConstrainedSamplingError, GrammarFormat, GrammarToolInputJsonBuffer,
    append_grammar_tool_input_json_delta, create_grammar_tool_input_properties,
    get_grammar_tool_input, get_json_schema_tool_parameters, make_strict_json_schema,
    resolve_grammar_constrained_sampling, resolve_json_schema_strict_sampling,
};
use pi_ai_providers::api::github_copilot_headers::{
    build_copilot_dynamic_headers, has_copilot_vision_input, infer_copilot_initiator,
};
use pi_ai_providers::api::openai_prompt_cache::{
    OPENAI_PROMPT_CACHE_KEY_MAX_LENGTH, clamp_openai_prompt_cache_key,
};
use serde_json::{Value, json};

fn schema(value: Value) -> JsonSchema {
    serde_json::from_value(value).unwrap()
}

fn tool(parameters: Value, constrained_sampling: Value) -> Tool {
    serde_json::from_value(json!({
        "name":"sample_tool", "description":"synthetic", "parameters":parameters,
        "constrainedSampling": constrained_sampling
    }))
    .unwrap()
}

fn string_parameters() -> Value {
    json!({"type":"object","properties":{"payload":{"type":"string"}},"required":["payload"]})
}

#[test]
fn strict_schema_source_fixtures() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/helpers/strict-schema.json"
    ))
    .unwrap();
    assert_eq!(fixture["evidenceClass"], "source-confirmed");
    assert_eq!(
        fixture["sourceCommit"],
        "853a80d26c90a14c1886f0ebb8ffaae133ca2185"
    );
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 8);
    for case in cases {
        let parameters = schema(case["schema"].clone());
        let before = parameters.clone();
        let strict = make_strict_json_schema(&parameters).unwrap();
        assert_eq!(
            serde_json::to_value(strict).unwrap(),
            case["expected"],
            "{}",
            case["id"]
        );
        assert_eq!(parameters, before);
    }
}

#[test]
fn strict_schema_rejections_are_atomic_and_prefer_only_swallows_unsupported() {
    let mut rejected = vec![
        json!({"type":"string"}),
        json!({"type":["object","null"]}),
        json!({"type":"object","anyOf":[]}),
        json!({"type":"object","anyOf":{}}),
        json!({"type":"object","anyOf":[true]}),
        json!({"type":"object","anyOf":[{"type":"object"}]}),
        json!({"type":"object","anyOf":[{"type":["array","null"]}]}),
        json!({"type":"object","anyOf":[{"items":{}}]}),
        json!({"type":"object","items":[]}),
        json!({"type":"object","items":null}),
        json!({"type":"object","properties":null}),
        json!({"type":"object","properties":[]}),
        json!({"type":"object","properties":{"a":true}}),
        json!({"type":"object","properties":{"a":{"properties":{}}}}),
        json!({"type":"object","additionalProperties":true}),
        json!({"type":"object","additionalProperties":null}),
        json!({"type":"object","additionalProperties":{"type":"string"}}),
        json!({"type":"object","required":null}),
        json!({"type":"object","required":[1]}),
        json!({"type":"object","required":["missing"]}),
    ];
    // Independently enumerated from the pinned exclusion list, including present null.
    for key in [
        "$ref",
        "$defs",
        "definitions",
        "allOf",
        "oneOf",
        "patternProperties",
        "dependentSchemas",
        "dependencies",
        "unevaluatedProperties",
        "propertyNames",
        "contains",
        "prefixItems",
        "not",
        "if",
        "then",
        "else",
    ] {
        let mut root = json!({"type":"object"});
        root[key] = Value::Null;
        rejected.push(root);
    }
    for parameters in rejected {
        let original = schema(parameters.clone());
        let before = original.clone();
        assert!(matches!(
            make_strict_json_schema(&original),
            Err(ConstrainedSamplingError::UnsupportedSchema(_))
        ));
        assert_eq!(original, before);
        let prefer = tool(
            parameters.clone(),
            json!({"type":"json_schema","strict":"prefer"}),
        );
        assert_eq!(
            resolve_json_schema_strict_sampling(&prefer, true).unwrap(),
            None
        );
        assert_eq!(
            resolve_json_schema_strict_sampling(&prefer, false).unwrap(),
            None
        );
        let require = tool(parameters, json!({"type":"json_schema","strict":"require"}));
        assert!(matches!(
            resolve_json_schema_strict_sampling(&require, true),
            Err(ConstrainedSamplingError::UnsupportedSchema(_))
        ));
        assert_eq!(
            resolve_json_schema_strict_sampling(&require, false),
            Err(ConstrainedSamplingError::StrictUnavailable)
        );
    }
    let strict = tool(
        string_parameters(),
        json!({"type":"json_schema","strict":"require"}),
    );
    assert_eq!(
        resolve_json_schema_strict_sampling(&strict, true).unwrap(),
        Some(true)
    );
    assert!(matches!(
        get_json_schema_tool_parameters(&strict, None).unwrap(),
        Cow::Borrowed(_)
    ));
    assert!(matches!(
        get_json_schema_tool_parameters(&strict, Some(false)).unwrap(),
        Cow::Borrowed(_)
    ));
    assert!(matches!(
        get_json_schema_tool_parameters(&strict, Some(true)).unwrap(),
        Cow::Owned(_)
    ));
    let disabled = tool(json!({}), json!(false));
    assert_eq!(
        resolve_json_schema_strict_sampling(&disabled, true).unwrap(),
        None
    );
    let mut omitted = disabled;
    omitted.constrained_sampling = None;
    assert_eq!(
        resolve_json_schema_strict_sampling(&omitted, false).unwrap(),
        None
    );
}

#[test]
fn schema_resource_bounds_are_exact_and_do_not_fall_back() {
    let overhead = r#"{"description":"","type":"object"}"#.len();
    for (size, succeeds) in [(256 * 1024, true), (256 * 1024 + 1, false)] {
        let parameters = json!({"type":"object","description":"a".repeat(size-overhead)});
        let input = schema(parameters.clone());
        let actual = make_strict_json_schema(&input);
        assert_eq!(actual.is_ok(), succeeds);
        if !succeeds {
            assert_eq!(actual.unwrap_err(), ConstrainedSamplingError::ResourceLimit);
            let prefer = tool(parameters, json!({"type":"json_schema","strict":"prefer"}));
            assert_eq!(
                resolve_json_schema_strict_sampling(&prefer, true),
                Err(ConstrainedSamplingError::ResourceLimit)
            );
        }
    }
    for (depth, succeeds) in [(128, true), (129, false)] {
        let mut value = JsonValue::Null;
        for _ in 1..depth {
            value = JsonValue::Array(vec![value]);
        }
        let input = JsonSchema(JsonObject::new(BTreeMap::from([
            ("type".to_owned(), JsonValue::String("object".to_owned())),
            ("default".to_owned(), value),
        ])));
        assert_eq!(make_strict_json_schema(&input).is_ok(), succeeds);
    }
}

#[test]
fn grammar_selection_input_shape_and_maps_follow_source() {
    let lark = "  start: /[a-z]+/  ";
    let mut input = tool(
        string_parameters(),
        json!({"type":"grammar","variants":{"openai_lark":lark,"openai_regex":"[a-z]+"}}),
    );
    let grammar = resolve_grammar_constrained_sampling(&input, true)
        .unwrap()
        .unwrap();
    assert_eq!(grammar.format, GrammarFormat::Lark);
    assert_eq!(grammar.definition, lark);
    assert_eq!(grammar.input_property, "payload");
    assert_eq!(
        serde_json::to_value(&grammar).unwrap(),
        json!({"format":"lark","definition":lark,"inputProperty":"payload"})
    );
    assert!(
        resolve_grammar_constrained_sampling(&input, false)
            .unwrap()
            .is_none()
    );
    for (whitespace, expected) in [
        (" \t\n\r\u{feff}\u{00a0}\u{2028}", GrammarFormat::Regex),
        ("\u{0085}", GrammarFormat::Lark),
    ] {
        let current = tool(
            string_parameters(),
            json!({"type":"grammar","variants":{"openai_lark":whitespace,"openai_regex":"x"}}),
        );
        assert_eq!(
            resolve_grammar_constrained_sampling(&current, true)
                .unwrap()
                .unwrap()
                .format,
            expected
        );
    }
    let missing = tool(json!({}), json!({"type":"grammar","variants":{}}));
    assert_eq!(
        resolve_grammar_constrained_sampling(&missing, true),
        Err(ConstrainedSamplingError::MissingGrammar)
    );
    assert!(
        resolve_grammar_constrained_sampling(&missing, false)
            .unwrap()
            .is_none()
    );
    for parameters in [
        json!({}),
        json!({"type":"object"}),
        json!({"type":"object","required":[]}),
        json!({"type":"object","required":["a","b"]}),
        json!({"type":"object","required":[false]}),
        json!({"type":"object","required":["a"],"properties":{}}),
        json!({"type":"object","required":["a"],"properties":{"a":{"type":["string","null"]}}}),
        json!({"type":"object","required":["a"],"properties":{"a":true}}),
    ] {
        input.parameters = schema(parameters);
        assert_eq!(
            resolve_grammar_constrained_sampling(&input, true),
            Err(ConstrainedSamplingError::GrammarInputShape)
        );
    }
    let first = tool(
        string_parameters(),
        json!({"type":"grammar","variants":{"openai_regex":"x"}}),
    );
    let second = tool(
        json!({"type":"object","required":["other"],"properties":{"other":{"type":"string"},"optional":{"type":"number"}}}),
        json!({"type":"grammar","variants":{"openai_regex":"x"}}),
    );
    assert_eq!(
        create_grammar_tool_input_properties(Some(&[first, second]), true).unwrap(),
        BTreeMap::from([("sample_tool".to_owned(), "other".to_owned())])
    );
    assert!(
        create_grammar_tool_input_properties(None, true)
            .unwrap()
            .is_empty()
    );
    assert!(
        create_grammar_tool_input_properties(Some(&[missing]), false)
            .unwrap()
            .is_empty()
    );
    let args: JsonObject = serde_json::from_value(json!({"payload":"abc"})).unwrap();
    assert_eq!(
        get_grammar_tool_input("sample_tool", &args, "payload").unwrap(),
        "abc"
    );
    for args in [json!({}), json!({"payload":42}), json!({"payload":null})] {
        let args = serde_json::from_value(args).unwrap();
        assert_eq!(
            get_grammar_tool_input("sample_tool", &args, "payload"),
            Err(ConstrainedSamplingError::GrammarInputValue)
        );
    }
}

#[test]
fn grammar_json_deltas_are_escaped_monotone_and_atomic() {
    let mut buffer = GrammarToolInputJsonBuffer::default();
    assert_eq!(
        append_grammar_tool_input_json_delta(&mut buffer, "payload", "", false).unwrap(),
        None
    );
    assert!(!buffer.started);
    let first = append_grammar_tool_input_json_delta(&mut buffer, "payload", "a\"", false)
        .unwrap()
        .unwrap();
    assert_eq!(first, "{\"payload\":\"a\\\"");
    let before = buffer.clone();
    assert_eq!(
        append_grammar_tool_input_json_delta(&mut buffer, "payload", "ab", false),
        Err(ConstrainedSamplingError::NonMonotoneInput)
    );
    assert_eq!(buffer, before);
    assert_eq!(
        append_grammar_tool_input_json_delta(&mut buffer, "payload", "a\"", false).unwrap(),
        None
    );
    let last = append_grammar_tool_input_json_delta(&mut buffer, "payload", "a\"\nb", true)
        .unwrap()
        .unwrap();
    assert_eq!(last, "\\nb\"}");
    assert_eq!(
        serde_json::from_str::<Value>(&(first + &last)).unwrap(),
        json!({"payload":"a\"\nb"})
    );
    let closed = buffer.clone();
    assert_eq!(
        append_grammar_tool_input_json_delta(&mut buffer, "payload", "a\"\nb", true).unwrap(),
        None
    );
    for (next, close) in [("a\"\nb", false), ("a\"\nb!", true), ("changed", true)] {
        assert_eq!(
            append_grammar_tool_input_json_delta(&mut buffer, "payload", next, close),
            Err(ConstrainedSamplingError::ClosedInput)
        );
        assert_eq!(buffer, closed);
    }
    let mut empty = GrammarToolInputJsonBuffer::default();
    assert_eq!(
        append_grammar_tool_input_json_delta(&mut empty, "", "", true)
            .unwrap()
            .unwrap(),
        "{\"\":\"\"}"
    );
    let mut escaped = GrammarToolInputJsonBuffer::default();
    let output = append_grammar_tool_input_json_delta(
        &mut escaped,
        "a\"\\\n",
        "\u{0000}\u{0008}\u{000c}\t\r\n\\\"\u{1f600}\u{2028}",
        true,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        output,
        "{\"a\\\"\\\\\\n\":\"\\u0000\\b\\f\\t\\r\\n\\\\\\\"\u{1f600}\u{2028}\"}"
    );
    let mut unicode = GrammarToolInputJsonBuffer::default();
    let first = append_grammar_tool_input_json_delta(&mut unicode, "p", "\u{1f600}", false)
        .unwrap()
        .unwrap();
    let second = append_grammar_tool_input_json_delta(&mut unicode, "p", "\u{1f600}é", true)
        .unwrap()
        .unwrap();
    assert_eq!(first + &second, "{\"p\":\"\u{1f600}é\"}");
}

#[test]
fn prompt_cache_counts_unicode_codepoints() {
    assert_eq!(OPENAI_PROMPT_CACHE_KEY_MAX_LENGTH, 64);
    assert_eq!(clamp_openai_prompt_cache_key(None), None);
    assert_eq!(clamp_openai_prompt_cache_key(Some("")), Some(""));
    for character in ["x", "é", "\u{1f600}"] {
        let at = character.repeat(64);
        let over = character.repeat(65);
        assert_eq!(clamp_openai_prompt_cache_key(Some(&at)), Some(at.as_str()));
        assert_eq!(
            clamp_openai_prompt_cache_key(Some(&over)),
            Some(at.as_str())
        );
    }
    let combining = "e\u{301}".repeat(33);
    let expected = "e\u{301}".repeat(32);
    assert_eq!(
        clamp_openai_prompt_cache_key(Some(&combining)),
        Some(expected.as_str())
    );
}

fn messages(values: Value) -> Vec<Message> {
    serde_json::from_value(values).unwrap()
}

#[test]
fn copilot_metadata_uses_last_role_and_all_user_tool_images() {
    let user = json!({"role":"user","content":"synthetic","timestamp":0});
    let user_image = json!({"role":"user","content":[{"type":"image","data":"AA==","mimeType":"image/png"}],"timestamp":0});
    let tool_image = json!({"role":"toolResult","toolName":"tool","toolCallId":"call","content":[{"type":"image","data":"AA==","mimeType":"image/png"}],"isError":false,"timestamp":0});
    let assistant = json!({"role":"assistant","api":"test","provider":"test","model":"test","content":[],"stopReason":"stop","timestamp":0,"usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}}});
    for (history, initiator, vision) in [
        (json!([]), "user", false),
        (json!([user]), "user", false),
        (json!([assistant]), "agent", false),
        (json!([tool_image]), "agent", true),
        (json!([user_image, user]), "user", true),
        (json!([assistant, user]), "user", false),
    ] {
        let history = messages(history);
        assert_eq!(infer_copilot_initiator(&history), initiator);
        assert_eq!(has_copilot_vision_input(&history), vision);
        let headers = build_copilot_dynamic_headers(&history, vision);
        assert_eq!(headers["X-Initiator"], initiator);
        assert_eq!(headers["Openai-Intent"], "conversation-edits");
        assert_eq!(
            headers.get("Copilot-Vision-Request").map(String::as_str),
            vision.then_some("true")
        );
    }
    assert!(
        !build_copilot_dynamic_headers(&messages(json!([user_image])), false)
            .contains_key("Copilot-Vision-Request")
    );
    assert_eq!(
        build_copilot_dynamic_headers(&[], true)["Copilot-Vision-Request"],
        "true"
    );
}

#[test]
fn helper_diagnostics_do_not_copy_input() {
    const SENTINEL: &str = "synthetic-private-tool-schema-argument";
    let input = tool(
        json!({"type":"object","required":[SENTINEL]}),
        json!({"type":"json_schema","strict":"require"}),
    );
    let error = resolve_json_schema_strict_sampling(&input, true).unwrap_err();
    assert!(!format!("{error:?} {error}").contains(SENTINEL));
    let args = JsonObject::default();
    let error = get_grammar_tool_input(SENTINEL, &args, SENTINEL).unwrap_err();
    assert!(!format!("{error:?} {error}").contains(SENTINEL));
    let mut buffer = GrammarToolInputJsonBuffer {
        input: SENTINEL.to_owned(),
        started: true,
        closed: true,
    };
    let error =
        append_grammar_tool_input_json_delta(&mut buffer, SENTINEL, "changed", true).unwrap_err();
    assert!(!format!("{error:?} {error} {buffer:?}").contains(SENTINEL));
    let grammar_tool = tool(
        string_parameters(),
        json!({"type":"grammar","variants":{"openai_regex":SENTINEL}}),
    );
    let grammar = resolve_grammar_constrained_sampling(&grammar_tool, true)
        .unwrap()
        .unwrap();
    assert!(!format!("{grammar:?}").contains(SENTINEL));
}
