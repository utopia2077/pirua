use pi_ai_core::{JsonSchema, Tool};
use pi_ai_providers::api::{constrained_sampling::*, wire_json::stringify_wire_json};
use serde_json::{Value, json};
#[test]
fn ordered_strict_parameters_match_executed_source_and_preserve_existing_slots() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/strict-schema-wire/trace.json"
    ))
    .unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let schema: JsonSchema = serde_json::from_value(case["input"].clone()).unwrap();
        let before = serde_json::to_value(&schema).unwrap();
        let wire = make_strict_wire_json_schema(&schema).unwrap().into_value();
        assert_eq!(
            stringify_wire_json(&wire).unwrap(),
            case["expectedJson"].as_str().unwrap(),
            "{}",
            case["id"]
        );
        assert_eq!(serde_json::to_value(&schema).unwrap(), before);
    }
}

fn tool(parameters: JsonSchema) -> Tool {
    let mut tool:Tool=serde_json::from_value(json!({"name":"lookup","description":"synthetic","parameters":{"type":"object"},"constrainedSampling":{"type":"json_schema","strict":"require"}})).unwrap();
    tool.parameters = parameters;
    tool
}
#[test]
fn wire_projection_preserves_canonical_values_errors_and_exact_existing_bounds() {
    use pi_ai_core::{JsonObject, JsonValue};
    use std::collections::BTreeMap;
    let old: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/helpers/strict-schema.json"
    ))
    .unwrap();
    for case in old["cases"].as_array().unwrap() {
        let schema: JsonSchema = serde_json::from_value(case["schema"].clone()).unwrap();
        assert_eq!(
            make_strict_wire_json_schema(&schema).unwrap().as_value(),
            &serde_json::to_value(make_strict_json_schema(&schema).unwrap()).unwrap()
        );
    }
    let mut invalid = vec![
        json!({"type":"string"}),
        json!({"type":"object","anyOf":[]}),
        json!({"type":"object","anyOf":[true]}),
        json!({"type":"object","anyOf":[{"type":"object"}]}),
        json!({"type":"object","items":[]}),
        json!({"type":"object","items":null}),
        json!({"type":"object","properties":null}),
        json!({"type":"object","properties":{"a":true}}),
        json!({"type":"object","properties":{"a":{"properties":{}}}}),
        json!({"type":"object","additionalProperties":true}),
        json!({"type":"object","required":["missing"]}),
    ];
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
        let mut schema = json!({"type":"object"});
        schema[key] = Value::Null;
        invalid.push(schema);
    }
    for value in invalid {
        let schema: JsonSchema = serde_json::from_value(value).unwrap();
        let before = schema.clone();
        assert_eq!(
            make_strict_wire_json_schema(&schema).unwrap_err(),
            make_strict_json_schema(&schema).unwrap_err()
        );
        assert_eq!(schema, before);
    }
    let overhead = r#"{"description":"","type":"object"}"#.len();
    for size in [256 * 1024, 256 * 1024 + 1] {
        let schema: JsonSchema = serde_json::from_value(
            json!({"description":"x".repeat(size-overhead),"type":"object"}),
        )
        .unwrap();
        assert_eq!(
            make_strict_wire_json_schema(&schema).is_ok(),
            size == 256 * 1024
        );
        assert_eq!(
            make_strict_wire_json_schema(&schema).err(),
            make_strict_json_schema(&schema).err()
        );
    }
    for depth in [128, 129] {
        let mut value = JsonValue::Null;
        for _ in 1..depth {
            value = JsonValue::Array(vec![value]);
        }
        let schema = JsonSchema(JsonObject::new(BTreeMap::from([
            ("type".into(), JsonValue::String("object".into())),
            ("default".into(), value),
        ])));
        assert_eq!(make_strict_wire_json_schema(&schema).is_ok(), depth == 128);
        assert_eq!(
            make_strict_wire_json_schema(&schema).err(),
            make_strict_json_schema(&schema).err()
        );
    }
}
#[test]
fn nonstrict_wire_copy_has_no_new_early_cap_and_strict_selection_is_explicit() {
    let schema: JsonSchema =
        serde_json::from_value(json!({"type":"object","description":"x".repeat(1_048_577)}))
            .unwrap();
    let tool = tool(schema);
    for strict in [None, Some(false)] {
        let wire = get_wire_json_schema_tool_parameters(&tool, strict).unwrap();
        assert_eq!(
            wire.as_value()["description"].as_str().unwrap().len(),
            1_048_577
        );
        assert!(matches!(
            get_json_schema_tool_parameters(&tool, strict).unwrap(),
            std::borrow::Cow::Borrowed(_)
        ));
    }
    assert_eq!(
        get_wire_json_schema_tool_parameters(&tool, Some(true)).unwrap_err(),
        ConstrainedSamplingError::ResourceLimit
    );
}
#[test]
fn responses_request_bytes_keep_equivalent_schemas_in_distinct_slots() {
    use pi_ai_providers::api::openai_responses_shared::{
        ConvertResponsesToolsOptions, convert_responses_tools,
    };
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/strict-schema-wire/trace.json"
    ))
    .unwrap();
    let mut bodies = vec![];
    for case in fixture["cases"].as_array().unwrap() {
        let tool = tool(serde_json::from_value(case["input"].clone()).unwrap());
        let tools =
            convert_responses_tools(&[tool], &ConvertResponsesToolsOptions::default()).unwrap();
        assert_eq!(
            stringify_wire_json(&tools[0]["parameters"]).unwrap(),
            case["expectedJson"].as_str().unwrap()
        );
        bodies.push(stringify_wire_json(&json!({"model":"synthetic","tools":tools})).unwrap());
    }
    assert_ne!(bodies[0], bodies[1]);
    assert_eq!(
        serde_json::from_str::<Value>(&bodies[0]).unwrap(),
        serde_json::from_str::<Value>(&bodies[1]).unwrap()
    );
}
