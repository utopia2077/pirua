use std::collections::BTreeMap;

use pi_ai_core::{
    FiniteF64, JsonNumber, JsonObject, JsonSchema, JsonValue, Tool, ToolCall, ToolCallId,
    ToolCallType, ToolName, ToolValidationError, validate_tool_arguments, validate_tool_call,
};

fn object(entries: impl IntoIterator<Item = (String, JsonValue)>) -> JsonObject {
    JsonObject::new(entries.into_iter().collect())
}

fn json_string(value: &str) -> JsonValue {
    JsonValue::String(value.to_owned())
}

fn json_number(value: f64) -> JsonValue {
    JsonValue::Number(JsonNumber::Float(
        FiniteF64::new(value).expect("test numbers are finite"),
    ))
}

fn tool(parameters: JsonObject) -> Tool {
    Tool {
        name: ToolName::new("echo").expect("valid tool name"),
        description: "Echo".to_owned(),
        parameters: JsonSchema(parameters),
        constrained_sampling: None,
    }
}

fn call(arguments: JsonObject) -> ToolCall {
    ToolCall {
        content_type: ToolCallType::ToolCall,
        id: ToolCallId::new("call-1").expect("valid tool call id"),
        name: ToolName::new("different-name").expect("valid tool name"),
        arguments,
        arguments_error: None,
        thought_signature: None,
        namespace: None,
    }
}

fn parse_object(source: &str) -> JsonObject {
    serde_json::from_str(source).expect("valid bounded JSON object")
}

fn parse_value(source: &str) -> JsonValue {
    serde_json::from_str(source).expect("valid bounded JSON value")
}

#[test]
fn explicit_model_feedback_preserves_parse_diagnostic_and_native_schema_details() {
    let declaration = tool(parse_object(
        r#"{"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}"#,
    ));
    let mut invocation = call(parse_object(r#"{"value":{"private":"received-secret"}}"#));
    invocation.name = declaration.name.clone();
    let error = validate_tool_arguments(&declaration, &invocation).unwrap_err();
    assert!(!format!("{error:?} {error}").contains("received-secret"));
    let feedback = pi_ai_core::format_tool_validation_feedback(&invocation, &error);
    assert!(feedback.starts_with("Validation failed for tool \"echo\":\n  - value [type]: "));
    assert!(feedback.contains("string"));
    assert!(feedback.contains("\n\nReceived arguments:\n"));
    assert!(feedback.contains("received-secret"));
    assert_eq!(
        invocation.arguments,
        parse_object(r#"{"value":{"private":"received-secret"}}"#)
    );

    invocation.arguments_error = Some("unterminated JSON".into());
    let error = validate_tool_arguments(&declaration, &invocation).unwrap_err();
    assert_eq!(
        pi_ai_core::format_tool_validation_feedback(&invocation, &error),
        "Invalid arguments for tool \"echo\": unterminated JSON"
    );
    assert_eq!(error.to_string(), "malformed tool arguments");
}

fn nested_object(depth: usize) -> JsonObject {
    let mut value = JsonValue::Object(JsonObject::default());
    for _ in 1..depth {
        value = JsonValue::Object(object([("nested".to_owned(), value)]));
    }
    let JsonValue::Object(value) = value else {
        unreachable!("nested object construction always returns an object");
    };
    value
}

fn pattern_properties_schema(count: usize) -> JsonValue {
    let patterns = (0..count)
        .map(|index| {
            (
                format!("pattern-{index}"),
                JsonValue::Object(JsonObject::default()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    JsonValue::Object(object([(
        "patternProperties".to_owned(),
        JsonValue::Object(JsonObject::new(patterns)),
    )]))
}

fn pattern_properties_with_source_bytes(extra_byte: bool) -> JsonValue {
    let patterns = (0..32)
        .map(|index| {
            let base_len = if extra_byte && index == 0 { 255 } else { 254 };
            (
                format!("{}{:02}", "a".repeat(base_len), index),
                JsonValue::Object(JsonObject::default()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    JsonValue::Object(object([(
        "patternProperties".to_owned(),
        JsonValue::Object(JsonObject::new(patterns)),
    )]))
}

#[test]
fn borrowed_preflight_enforces_depth_and_exact_size_limits() {
    let permissive = tool(JsonObject::default());
    assert!(
        validate_tool_arguments(&tool(nested_object(128)), &call(JsonObject::default())).is_ok()
    );
    assert_eq!(
        validate_tool_arguments(&tool(nested_object(129)), &call(JsonObject::default())),
        Err(ToolValidationError::InvalidSchema)
    );

    let depth_128 = call(nested_object(128));
    assert_eq!(
        validate_tool_arguments(&permissive, &depth_128),
        Ok(depth_128.arguments.clone())
    );

    let depth_129 = call(nested_object(129));
    assert!(matches!(
        validate_tool_arguments(&permissive, &depth_129),
        Err(ToolValidationError::InvalidArguments(_))
    ));

    let exact_argument = call(object([(
        "x".to_owned(),
        json_string(&"a".repeat(1024 * 1024 - 8)),
    )]));
    assert!(validate_tool_arguments(&permissive, &exact_argument).is_ok());
    let oversized_argument = call(object([(
        "x".to_owned(),
        json_string(&"a".repeat(1024 * 1024 - 7)),
    )]));
    assert!(matches!(
        validate_tool_arguments(&permissive, &oversized_argument),
        Err(ToolValidationError::InvalidArguments(_))
    ));

    let exact_schema = tool(object([(
        "description".to_owned(),
        json_string(&"a".repeat(256 * 1024 - 18)),
    )]));
    assert!(
        validate_tool_arguments(&exact_schema, &call(JsonObject::default())).is_ok(),
        "a schema with an exact 256 KiB encoding is accepted"
    );
    let oversized_schema = tool(object([(
        "description".to_owned(),
        json_string(&"a".repeat(256 * 1024 - 17)),
    )]));
    assert_eq!(
        validate_tool_arguments(&oversized_schema, &call(JsonObject::default())),
        Err(ToolValidationError::InvalidSchema)
    );
}

#[test]
fn schema_preflight_rejects_external_references_and_excess_patterns() {
    let external = tool(parse_object(r##"{"$ref":"https://example.test/schema"}"##));
    assert_eq!(
        validate_tool_arguments(&external, &call(JsonObject::default())),
        Err(ToolValidationError::InvalidSchema)
    );

    let local = tool(parse_object(
        r##"{"$ref":"#/$defs/args","$defs":{"args":{"type":"object"}}}"##,
    ));
    assert!(validate_tool_arguments(&local, &call(JsonObject::default())).is_ok());

    let keyword_named_properties = tool(parse_object(
        r#"{"type":"object","properties":{"$ref":{"type":"string"},"$dynamicRef":{"type":"string"},"$recursiveRef":{"type":"string"},"patternProperties":{"type":"integer"}},"required":["$ref","$dynamicRef","$recursiveRef","patternProperties"]}"#,
    ));
    let keyword_named_arguments = call(parse_object(
        r#"{"$ref":"ok","$dynamicRef":"ok","$recursiveRef":"ok","patternProperties":7}"#,
    ));
    assert_eq!(
        validate_tool_arguments(&keyword_named_properties, &keyword_named_arguments),
        Ok(keyword_named_arguments.arguments.clone()),
        "property names are data, not schema keywords"
    );

    let keyword_named_definitions = tool(parse_object(
        r##"{"type":"object","definitions":{"$ref":{"type":"string"},"patternProperties":{"type":"number"}},"properties":{"value":{"$ref":"#/definitions/$ref"}}}"##,
    ));
    assert!(
        validate_tool_arguments(
            &keyword_named_definitions,
            &call(parse_object(r#"{"value":"ok"}"#))
        )
        .is_ok(),
        "definition names are not schema keywords"
    );

    let keyword_named_defs_2020 = tool(parse_object(
        r##"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","$defs":{"$dynamicRef":{"type":"string"},"patternProperties":{"type":"number"}},"properties":{"value":{"$ref":"#/$defs/$dynamicRef"}}}"##,
    ));
    assert!(
        validate_tool_arguments(
            &keyword_named_defs_2020,
            &call(parse_object(r#"{"value":"ok"}"#))
        )
        .is_ok(),
        "$defs names are not schema keywords"
    );

    let keyword_named_dependencies = tool(parse_object(
        r#"{"type":"object","dependencies":{"$ref":["other"]},"properties":{"$ref":{"type":"string"},"other":{"type":"string"}}}"#,
    ));
    assert!(
        validate_tool_arguments(
            &keyword_named_dependencies,
            &call(parse_object(r#"{"$ref":"ok","other":"ok"}"#))
        )
        .is_ok(),
        "property-dependency names and string arrays remain data"
    );

    let keyword_named_dependent_schema = tool(parse_object(
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"$ref":{"type":"string"},"other":{"type":"string"}},"dependentSchemas":{"$ref":{"required":["other"]}}}"#,
    ));
    assert!(
        validate_tool_arguments(
            &keyword_named_dependent_schema,
            &call(parse_object(r#"{"$ref":"ok","other":"ok"}"#))
        )
        .is_ok(),
        "dependentSchemas keys are names while values remain schemas"
    );

    let keyword_named_pattern = tool(parse_object(
        r#"{"type":"object","patternProperties":{"\\$ref$":{"type":"string"},"patternProperties":{"type":"integer"}}}"#,
    ));
    assert!(
        validate_tool_arguments(
            &keyword_named_pattern,
            &call(parse_object(r#"{"patternProperties":7}"#))
        )
        .is_ok(),
        "patternProperties keys are regexes, not schema keywords"
    );

    let instance_data = r##"{"$ref":"https://data.invalid/not-a-schema","pattern":"(","patternProperties":"literal"}"##;
    let data_keywords = tool(parse_object(&format!(
        r#"{{"type":"object","const":{instance_data},"enum":[{instance_data}],"default":{instance_data},"examples":[{instance_data}],"x-extension":{instance_data}}}"#
    )));
    let data_arguments = call(parse_object(instance_data));
    assert_eq!(
        validate_tool_arguments(&data_keywords, &data_arguments),
        Ok(data_arguments.arguments.clone()),
        "instance-valued keywords and unknown extensions remain data frames"
    );

    let nested_external = tool(parse_object(
        r#"{"type":"object","properties":{"value":{"$ref":"https://example.test/nested"}}}"#,
    ));
    assert_eq!(
        validate_tool_arguments(&nested_external, &call(parse_object(r#"{"value":null}"#))),
        Err(ToolValidationError::InvalidSchema),
        "recognized applicators still enter schema frames"
    );

    let patterns = |count: usize| {
        let entries = (0..count)
            .map(|index| {
                (
                    format!("pattern-{index}"),
                    JsonValue::Object(JsonObject::default()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        tool(object([(
            "patternProperties".to_owned(),
            JsonValue::Object(JsonObject::new(entries)),
        )]))
    };
    assert!(validate_tool_arguments(&patterns(32), &call(JsonObject::default())).is_ok());
    assert_eq!(
        validate_tool_arguments(&patterns(33), &call(JsonObject::default())),
        Err(ToolValidationError::InvalidSchema)
    );

    let patterns_with_source_bytes = |extra_byte: bool| {
        let entries = (0..32)
            .map(|index| {
                let base_len = if extra_byte && index == 0 { 255 } else { 254 };
                (
                    format!("{}{:02}", "a".repeat(base_len), index),
                    JsonValue::Object(JsonObject::default()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        tool(object([(
            "patternProperties".to_owned(),
            JsonValue::Object(JsonObject::new(entries)),
        )]))
    };
    assert!(
        validate_tool_arguments(
            &patterns_with_source_bytes(false),
            &call(JsonObject::default())
        )
        .is_ok()
    );
    assert_eq!(
        validate_tool_arguments(
            &patterns_with_source_bytes(true),
            &call(JsonObject::default())
        ),
        Err(ToolValidationError::InvalidSchema)
    );
}

#[test]
fn schema_preflight_follows_local_reference_closure() {
    let catastrophic = format!("{}!", "ab".repeat(28));
    let referenced_runtime_pattern = tool(parse_object(
        r##"{"$ref":"#/default","default":{"type":"object","patternProperties":{"^(a|b|ab)*\\1$":false}}}"##,
    ));
    let error = validate_tool_arguments(
        &referenced_runtime_pattern,
        &call(object([(catastrophic, JsonValue::Null)])),
    )
    .expect_err("a reference-promoted pattern remains under the runtime guard");
    let ToolValidationError::InvalidArguments(report) = error else {
        panic!("expected invalid arguments");
    };
    assert_eq!(report.issues()[0].path(), "root");
    assert_eq!(report.issues()[0].keyword(), "patternProperties");
    assert_eq!(
        report.issues()[0].message(),
        "pattern evaluation limit exceeded"
    );

    for count in [32, 33] {
        let schema = tool(object([
            ("$ref".to_owned(), json_string("#/default")),
            ("default".to_owned(), pattern_properties_schema(count)),
        ]));
        let result = validate_tool_arguments(&schema, &call(JsonObject::default()));
        if count == 32 {
            assert!(result.is_ok(), "32 physical patterns remain accepted");
        } else {
            assert_eq!(result, Err(ToolValidationError::InvalidSchema));
        }
    }

    let repeated_target = tool(object([
        (
            "allOf".to_owned(),
            JsonValue::Array(vec![
                JsonValue::Object(object([("$ref".to_owned(), json_string("#/default"))])),
                JsonValue::Object(object([("$ref".to_owned(), json_string("#/default"))])),
            ]),
        ),
        ("default".to_owned(), pattern_properties_schema(32)),
    ]));
    assert!(
        validate_tool_arguments(&repeated_target, &call(JsonObject::default())).is_ok(),
        "multiple incoming references do not double-count physical patterns"
    );

    let distinct_targets = tool(object([
        (
            "allOf".to_owned(),
            JsonValue::Array(vec![
                JsonValue::Object(object([("$ref".to_owned(), json_string("#/left"))])),
                JsonValue::Object(object([("$ref".to_owned(), json_string("#/right"))])),
            ]),
        ),
        ("left".to_owned(), pattern_properties_schema(16)),
        ("right".to_owned(), pattern_properties_schema(17)),
    ]));
    assert_eq!(
        validate_tool_arguments(&distinct_targets, &call(JsonObject::default())),
        Err(ToolValidationError::InvalidSchema),
        "distinct physical targets contribute independently to the pattern bound"
    );

    for extra_byte in [false, true] {
        let schema = tool(object([
            ("$ref".to_owned(), json_string("#/default")),
            (
                "default".to_owned(),
                pattern_properties_with_source_bytes(extra_byte),
            ),
        ]));
        let result = validate_tool_arguments(&schema, &call(JsonObject::default()));
        if extra_byte {
            assert_eq!(result, Err(ToolValidationError::InvalidSchema));
        } else {
            assert!(
                result.is_ok(),
                "exactly 8 KiB of pattern source is accepted"
            );
        }
    }

    let unreferenced_data = tool(object([(
        "default".to_owned(),
        pattern_properties_schema(33),
    )]));
    assert!(
        validate_tool_arguments(&unreferenced_data, &call(JsonObject::default())).is_ok(),
        "unreferenced instance-valued data remains outside the schema graph"
    );

    let transitive_external = tool(parse_object(
        r##"{"$ref":"#/a","a":{"$ref":"#/b"},"b":{"$ref":"https://example.invalid/schema"}}"##,
    ));
    assert_eq!(
        validate_tool_arguments(&transitive_external, &call(JsonObject::default())),
        Err(ToolValidationError::InvalidSchema)
    );

    let cyclic = tool(object([
        ("$ref".to_owned(), json_string("#/a")),
        (
            "a".to_owned(),
            JsonValue::Object(object([("$ref".to_owned(), json_string("#/b"))])),
        ),
        (
            "b".to_owned(),
            JsonValue::Object(object([
                ("$ref".to_owned(), json_string("#/a")),
                (
                    "allOf".to_owned(),
                    JsonValue::Array(vec![pattern_properties_schema(32)]),
                ),
            ])),
        ),
    ]));
    assert!(
        validate_tool_arguments(&cyclic, &call(JsonObject::default())).is_ok(),
        "reference cycles terminate without duplicate pattern accounting"
    );
}

#[test]
fn schema_preflight_many_reference_contexts_reuse_resolver_scope() {
    let references = (0..4_096)
        .map(|_| JsonValue::Object(object([("$ref".to_owned(), json_string("#/default"))])))
        .collect();
    let schema = tool(object([
        ("allOf".to_owned(), JsonValue::Array(references)),
        (
            "default".to_owned(),
            JsonValue::Object(JsonObject::default()),
        ),
    ]));
    assert!(
        validate_tool_arguments(&schema, &call(JsonObject::default())).is_ok(),
        "thousands of contexts reuse one interned base URI and dynamic scope"
    );
}

#[test]
fn schema_preflight_resolves_pointer_and_resource_scopes() {
    for reference in ["#/%64efault", "#/a~1b", "#/a~0b", "#/array/0"] {
        let schema = tool(object([
            ("$ref".to_owned(), json_string(reference)),
            ("default".to_owned(), pattern_properties_schema(33)),
            ("a/b".to_owned(), pattern_properties_schema(33)),
            ("a~b".to_owned(), pattern_properties_schema(33)),
            (
                "array".to_owned(),
                JsonValue::Array(vec![pattern_properties_schema(33)]),
            ),
        ]));
        assert_eq!(
            validate_tool_arguments(&schema, &call(JsonObject::default())),
            Err(ToolValidationError::InvalidSchema),
            "local pointer {reference:?} promotes its exact target"
        );
    }

    let encoded_separator = tool(object([
        ("$ref".to_owned(), json_string("#/a%2Fb")),
        (
            "a".to_owned(),
            JsonValue::Object(object([("b".to_owned(), pattern_properties_schema(33))])),
        ),
        ("a/b".to_owned(), JsonValue::Object(JsonObject::default())),
    ]));
    assert_eq!(
        validate_tool_arguments(&encoded_separator, &call(JsonObject::default())),
        Err(ToolValidationError::InvalidSchema),
        "percent decoding precedes pointer segmentation"
    );

    for reference in ["#/%FF", "#/missing", "#/array/not-an-index", "#/array/2"] {
        let schema = tool(object([
            ("$ref".to_owned(), json_string(reference)),
            (
                "array".to_owned(),
                JsonValue::Array(vec![JsonValue::Object(JsonObject::default())]),
            ),
        ]));
        assert_eq!(
            validate_tool_arguments(&schema, &call(JsonObject::default())),
            Err(ToolValidationError::InvalidSchema),
            "invalid local pointer {reference:?} remains a typed schema error"
        );
    }

    let embedded_resource = tool(parse_object(
        r##"{
            "$id":"https://example.test/root",
            "$ref":"#/$defs/embedded",
            "$defs":{
                "embedded":{
                    "$id":"child",
                    "$ref":"#/default",
                    "default":{
                        "patternProperties":{
                            "p00":{},"p01":{},"p02":{},"p03":{},"p04":{},"p05":{},
                            "p06":{},"p07":{},"p08":{},"p09":{},"p10":{},"p11":{},
                            "p12":{},"p13":{},"p14":{},"p15":{},"p16":{},"p17":{},
                            "p18":{},"p19":{},"p20":{},"p21":{},"p22":{},"p23":{},
                            "p24":{},"p25":{},"p26":{},"p27":{},"p28":{},"p29":{},
                            "p30":{},"p31":{},"p32":{}
                        }
                    }
                }
            }
        }"##,
    ));
    assert_eq!(
        validate_tool_arguments(&embedded_resource, &call(JsonObject::default())),
        Err(ToolValidationError::InvalidSchema),
        "fragment pointers resolve from the active embedded resource root"
    );

    let relative_resource_anchor = tool(parse_object(
        r##"{
            "$schema":"https://json-schema.org/draft/2020-12/schema",
            "$id":"https://example.test/root/",
            "$ref":"#/$defs/embedded",
            "$defs":{
                "embedded":{
                    "$id":"sub/",
                    "$ref":"#node",
                    "$defs":{"target":{"$anchor":"node","type":"object"}}
                }
            }
        }"##,
    ));
    assert!(
        validate_tool_arguments(&relative_resource_anchor, &call(JsonObject::default())).is_ok(),
        "a resolved target does not apply its relative resource id twice"
    );

    let promoted_data_resource = tool(parse_object(
        r##"{
            "$schema":"https://json-schema.org/draft/2020-12/schema",
            "$id":"https://example.test/root/",
            "$ref":"#/default",
            "localData":true,
            "default":{
                "$id":"sub/",
                "$ref":"#/localData",
                "localData":{"$ref":"https://external.invalid/schema"}
            }
        }"##,
    ));
    assert!(
        validate_tool_arguments(&promoted_data_resource, &call(JsonObject::default())).is_ok(),
        "a data-path target uses the containing scope returned by the pinned resolver"
    );

    let promoted_direct_external = tool(parse_object(
        r##"{
            "$schema":"https://json-schema.org/draft/2020-12/schema",
            "$id":"https://example.test/root/",
            "$ref":"#/default",
            "default":{"$id":"sub/","$ref":"https://external.invalid/schema"}
        }"##,
    ));
    assert_eq!(
        validate_tool_arguments(&promoted_direct_external, &call(JsonObject::default())),
        Err(ToolValidationError::InvalidSchema),
        "an active external ref on a data-path target still fails closed"
    );
}

#[test]
fn schema_preflight_dynamic_scope_cycle_terminates() {
    let recursive = tool(parse_object(
        r##"{
            "$schema":"https://json-schema.org/draft/2020-12/schema",
            "$id":"https://example.test/root",
            "$dynamicAnchor":"node",
            "$defs":{
                "child":{
                    "$id":"child",
                    "$dynamicAnchor":"node",
                    "$dynamicRef":"#node"
                }
            },
            "$ref":"#/$defs/child"
        }"##,
    ));
    assert!(validate_tool_arguments(&recursive, &call(JsonObject::default())).is_ok());
}

#[test]
fn schema_preflight_uses_selected_draft_keyword_semantics() {
    let draft_4_unknown_applicators = tool(object([
        (
            "$schema".to_owned(),
            json_string("https://json-schema.org/draft-04/schema"),
        ),
        (
            "prefixItems".to_owned(),
            JsonValue::Array(vec![pattern_properties_schema(33)]),
        ),
        ("$defs".to_owned(), pattern_properties_schema(33)),
        (
            "$dynamicRef".to_owned(),
            json_string("https://data.invalid/not-a-draft-4-keyword"),
        ),
        (
            "$recursiveRef".to_owned(),
            json_string("https://data.invalid/not-a-draft-4-keyword"),
        ),
    ]));
    assert!(
        validate_tool_arguments(&draft_4_unknown_applicators, &call(JsonObject::default())).is_ok(),
        "Draft 4 leaves the listed later-draft applicator and ref keywords as data"
    );

    let draft_4_registry_ref_source = r##"{
        "$schema":"https://json-schema.org/draft-04/schema",
        "$defs":{"ignored":{"$ref":"https://data.invalid/registry-crawl"}}
    }"##;
    let direct_schema: serde_json::Value =
        serde_json::from_str(draft_4_registry_ref_source).expect("valid JSON");
    assert!(
        jsonschema::options()
            .offline()
            .with_draft(jsonschema::Draft::Draft4)
            .build(&direct_schema)
            .is_err(),
        "the pinned registry conservatively crawls this later-draft location"
    );
    assert_eq!(
        validate_tool_arguments(
            &tool(parse_object(draft_4_registry_ref_source)),
            &call(JsonObject::default())
        ),
        Err(ToolValidationError::InvalidSchema),
        "the wrapper preserves the pinned registry's fail-closed build result"
    );

    let draft_7_unknown_applicators = tool(object([
        (
            "prefixItems".to_owned(),
            JsonValue::Array(vec![pattern_properties_schema(33)]),
        ),
        ("$defs".to_owned(), pattern_properties_schema(33)),
        (
            "$dynamicRef".to_owned(),
            json_string("https://data.invalid/not-a-draft-7-keyword"),
        ),
        (
            "$recursiveRef".to_owned(),
            json_string("https://data.invalid/not-a-draft-7-keyword"),
        ),
    ]));
    assert!(
        validate_tool_arguments(&draft_7_unknown_applicators, &call(JsonObject::default())).is_ok(),
        "Draft 7 leaves later-draft keywords as unreferenced data"
    );

    let draft_7_reference_promotion = tool(object([
        ("$ref".to_owned(), json_string("#/$defs")),
        ("$defs".to_owned(), pattern_properties_schema(33)),
    ]));
    assert_eq!(
        validate_tool_arguments(&draft_7_reference_promotion, &call(JsonObject::default())),
        Err(ToolValidationError::InvalidSchema),
        "an active Draft 7 ref still promotes a target stored under an unknown keyword"
    );

    let draft_2019_unknown_prefix_items = tool(object([
        (
            "$schema".to_owned(),
            json_string("https://json-schema.org/draft/2019-09/schema"),
        ),
        (
            "prefixItems".to_owned(),
            JsonValue::Array(vec![pattern_properties_schema(33)]),
        ),
    ]));
    assert!(
        validate_tool_arguments(
            &draft_2019_unknown_prefix_items,
            &call(JsonObject::default())
        )
        .is_ok(),
        "prefixItems is not an applicator before Draft 2020-12"
    );

    let draft_2020_prefix_items = tool(object([
        (
            "$schema".to_owned(),
            json_string("https://json-schema.org/draft/2020-12/schema"),
        ),
        (
            "prefixItems".to_owned(),
            JsonValue::Array(vec![pattern_properties_schema(33)]),
        ),
    ]));
    assert_eq!(
        validate_tool_arguments(&draft_2020_prefix_items, &call(JsonObject::default())),
        Err(ToolValidationError::InvalidSchema)
    );

    let draft_2020_external_dynamic_ref = tool(parse_object(
        r##"{"$schema":"https://json-schema.org/draft/2020-12/schema","$dynamicRef":"https://data.invalid/active-keyword"}"##,
    ));
    assert_eq!(
        validate_tool_arguments(
            &draft_2020_external_dynamic_ref,
            &call(JsonObject::default())
        ),
        Err(ToolValidationError::InvalidSchema)
    );
}

#[test]
fn pattern_compile_backtrack_and_dfa_boundaries_are_typed_and_bounded() {
    let compiled_too_large = tool(parse_object(
        r#"{"type":"object","propertyNames":{"pattern":"^.{0,404600}$"}}"#,
    ));
    assert_eq!(
        validate_tool_arguments(&compiled_too_large, &call(JsonObject::default())),
        Err(ToolValidationError::InvalidSchema)
    );

    let catastrophic = format!("{}!", "ab".repeat(28));
    let ordinary = validate_value(
        r#"{"type":"string","pattern":"^(a|b|ab)*\\1$"}"#,
        json_string(&catastrophic),
    )
    .expect_err("ordinary pattern exhausts the fixed backtrack budget");
    assert!(matches!(ordinary, ToolValidationError::InvalidArguments(_)));

    let pattern_properties = tool(parse_object(
        r#"{"type":"object","patternProperties":{"^(a|b|ab)*\\1$":false}}"#,
    ));
    let arguments = object([(catastrophic.clone(), JsonValue::Null)]);
    let error = validate_tool_arguments(&pattern_properties, &call(arguments))
        .expect_err("patternProperties backtracking is not swallowed as a non-match");
    let ToolValidationError::InvalidArguments(report) = error else {
        panic!("expected invalid arguments");
    };
    assert_eq!(report.issues()[0].path(), "root");
    assert_eq!(report.issues()[0].keyword(), "patternProperties");
    assert_eq!(
        report.issues()[0].message(),
        "pattern evaluation limit exceeded"
    );

    let unused_pattern_properties = tool(parse_object(
        r#"{"type":"object","definitions":{"unused":{"type":"object","patternProperties":{"^(a|b|ab)*\\1$":false}}}}"#,
    ));
    let error = validate_tool_arguments(
        &unused_pattern_properties,
        &call(object([(catastrophic.clone(), JsonValue::Null)])),
    )
    .expect_err("the accepted fail-closed guard includes unreachable schema branches");
    let ToolValidationError::InvalidArguments(report) = error else {
        panic!("expected invalid arguments");
    };
    assert_eq!(report.issues()[0].keyword(), "patternProperties");
    assert_eq!(
        report.issues()[0].message(),
        "pattern evaluation limit exceeded"
    );

    let mut dfa_input = String::with_capacity(100_021);
    for index in 0..100_000 {
        dfa_input.push(if index % 3 == 0 { '1' } else { '0' });
    }
    dfa_input.push('1');
    for index in 0..20 {
        dfa_input.push(if index % 3 == 0 { '1' } else { '0' });
    }
    assert_eq!(
        validate_value(
            r#"{"type":"string","pattern":"[01]*1[01]{20}$"}"#,
            json_string(&dfa_input)
        ),
        Ok(json_string(&dfa_input)),
        "DFA cache exhaustion falls back without changing the result"
    );
    assert!(matches!(
        validate_value(
            r#"{"type":"string","pattern":"[01]*1[01]{20}$"}"#,
            json_string(&"0".repeat(100_021))
        ),
        Err(ToolValidationError::InvalidArguments(_))
    ));
}

#[test]
fn public_errors_are_redacted_bounded_and_case_sensitive() {
    let property = "p".repeat(300);
    let schema_source = format!(r#"{{"type":"object","required":["{property}"]}}"#);
    let echo = tool(parse_object(&schema_source));
    let arguments = object([("value".to_owned(), json_string("never expose this value"))]);
    let mut matching_call = call(arguments);
    matching_call.name = ToolName::new("echo").expect("valid tool name");

    let error = validate_tool_call(&[echo], &matching_call)
        .expect_err("the required property is absent from the arguments");
    assert_eq!(error.to_string(), "tool arguments failed validation");
    assert!(!format!("{error:?}").contains("never expose this value"));
    let ToolValidationError::InvalidArguments(report) = error else {
        panic!("expected an invalid-arguments report");
    };
    assert_eq!(report.issues().len(), 1);
    assert!(report.issues()[0].path().starts_with('p'));
    assert!(report.issues()[0].path().len() <= 256);
    assert_eq!(report.issues()[0].keyword(), "required");
    assert_eq!(report.issues()[0].message(), "required property is missing");

    matching_call.name = ToolName::new("Echo").expect("valid tool name");
    assert_eq!(
        validate_tool_call(&[tool(JsonObject::default())], &matching_call),
        Err(ToolValidationError::ToolNotFound)
    );
}

#[test]
fn optional_null_normalization_handles_properties_items_and_direct_refs() {
    let schema = tool(parse_object(
        r##"{
            "type":"object",
            "properties":{
                "drop":{"type":"string"},
                "nullable":{"type":["string","null"]},
                "referenced":{"$ref":"#/$defs/nullable"},
                "nested":{"type":"object","properties":{"drop":{"type":"number"}}},
                "list":{"type":"array","items":{"type":"object","properties":{"drop":{"type":"boolean"}}}}
            },
            "$defs":{"nullable":{"type":["string","null"]}}
        }"##,
    ));
    let arguments = parse_object(
        r#"{
            "drop":null,
            "nullable":null,
            "referenced":null,
            "nested":{"drop":null},
            "list":[{"drop":null}]
        }"#,
    );
    let expected = parse_object(
        r#"{
            "nullable":null,
            "referenced":null,
            "nested":{},
            "list":[{}]
        }"#,
    );
    assert_eq!(
        validate_tool_arguments(&schema, &call(arguments)),
        Ok(expected)
    );
}

fn validate_value(schema: &str, input: JsonValue) -> Result<JsonValue, ToolValidationError> {
    let parameters =
        format!(r#"{{"type":"object","properties":{{"value":{schema}}},"required":["value"]}}"#);
    let arguments = object([("value".to_owned(), input)]);
    validate_tool_arguments(&tool(parse_object(&parameters)), &call(arguments)).map(|arguments| {
        arguments
            .into_map()
            .remove("value")
            .expect("the required value remains present")
    })
}

#[test]
fn primitive_coercion_uses_ecmascript_number_and_string_rules() {
    let number_cases = [
        ("\u{feff}42\u{2029}", parse_value("42")),
        ("+.5", json_number(0.5)),
        ("1.", parse_value("1")),
        ("-1e2", parse_value("-100")),
        ("0x10", parse_value("16")),
        ("0b101", parse_value("5")),
        ("0o10", parse_value("8")),
        ("1e-324", parse_value("0")),
        ("9007199254740993", json_number(9_007_199_254_740_992.0)),
        ("0x20000000000001", json_number(9_007_199_254_740_992.0)),
        ("0x20000000000003", json_number(9_007_199_254_740_996.0)),
    ];
    for (input, expected) in number_cases {
        assert_eq!(
            validate_value(r#"{"type":"number"}"#, json_string(input)),
            Ok(expected),
            "numeric input {input:?}"
        );
    }

    for input in [
        "+0x1",
        "0x",
        "0b2",
        "0o8",
        " ",
        "1_000",
        "NaN",
        "Infinity",
        "1e309",
        "42\u{0085}",
        &format!("0x{}", "f".repeat(256)),
    ] {
        assert!(matches!(
            validate_value(r#"{"type":"number"}"#, json_string(input)),
            Err(ToolValidationError::InvalidArguments(_))
        ));
    }

    assert_eq!(
        validate_value(r#"{"type":"boolean"}"#, JsonValue::Null),
        Ok(JsonValue::Bool(false))
    );
    assert_eq!(
        validate_value(r#"{"type":"number"}"#, JsonValue::Bool(false)),
        Ok(parse_value("0"))
    );
    assert_eq!(
        validate_value(r#"{"type":"integer"}"#, JsonValue::Null),
        Ok(parse_value("0"))
    );
    assert_eq!(
        validate_value(r#"{"type":"integer"}"#, JsonValue::Bool(true)),
        Ok(parse_value("1"))
    );
    assert_eq!(
        validate_value(r#"{"type":"integer"}"#, json_string("0x10")),
        Ok(parse_value("16"))
    );
    assert_eq!(
        validate_value(r#"{"type":"number"}"#, json_string("-0")),
        Ok(json_number(-0.0))
    );
    assert_eq!(
        validate_value(r#"{"type":"boolean"}"#, json_string("true")),
        Ok(JsonValue::Bool(true))
    );
    assert_eq!(
        validate_value(r#"{"type":"null"}"#, json_string("")),
        Ok(JsonValue::Null)
    );
    for (schema, input) in [
        (r#"{"type":"boolean"}"#, json_string("1")),
        (r#"{"type":"boolean"}"#, json_string("0")),
        (r#"{"type":"integer"}"#, json_string("42.1")),
        (r#"{"type":"null"}"#, json_string("null")),
    ] {
        assert!(matches!(
            validate_value(schema, input),
            Err(ToolValidationError::InvalidArguments(_))
        ));
    }

    let string_cases = [
        (json_number(-0.0), "0"),
        (json_number(1e-6), "0.000001"),
        (json_number(1e-7), "1e-7"),
        (json_number(1e20), "100000000000000000000"),
        (json_number(1e21), "1e+21"),
    ];
    for (input, expected) in string_cases {
        assert_eq!(
            validate_value(r#"{"type":"string"}"#, input),
            Ok(json_string(expected))
        );
    }
}

#[test]
fn composite_coercion_preserves_unions_and_recurses_in_source_order() {
    assert_eq!(
        validate_value(
            r#"{"allOf":[{"type":["number","boolean"]},{"type":["boolean","number"]}]}"#,
            JsonValue::Null
        ),
        Ok(parse_value("0")),
        "allOf coercion runs in source order"
    );
    assert_eq!(
        validate_value(r#"{"type":["number","string"]}"#, json_string("1")),
        Ok(json_string("1"))
    );
    assert_eq!(
        validate_value(r#"{"type":["boolean","number"]}"#, json_string("1")),
        Ok(parse_value("1"))
    );
    assert_eq!(
        validate_value(
            r#"{"anyOf":[{"type":"number"},{"type":"null"}]}"#,
            JsonValue::Null
        ),
        Ok(JsonValue::Null)
    );
    assert_eq!(
        validate_value(
            r#"{"oneOf":[{"type":"number"},{"type":"null"}]}"#,
            JsonValue::Null
        ),
        Ok(JsonValue::Null)
    );
    assert_eq!(
        validate_value(
            r#"{"anyOf":[{"type":"number"},{"type":"null"}]}"#,
            json_string("42")
        ),
        Ok(parse_value("42"))
    );
    assert!(matches!(
        validate_value(
            r#"{"oneOf":[{"type":"number"},{"type":["number","boolean"]}]}"#,
            json_string("42")
        ),
        Err(ToolValidationError::InvalidArguments(_))
    ));
    assert_eq!(
        validate_value(
            r#"{"anyOf":[{"type":"number","minimum":10},{"type":"string","pattern":"^true$"}]}"#,
            JsonValue::Bool(true)
        ),
        Ok(json_string("true"))
    );
    assert!(matches!(
        validate_value(
            r#"{"allOf":[{"type":"number"},{"type":"integer"}]}"#,
            json_string("1.5")
        ),
        Err(ToolValidationError::InvalidArguments(_))
    ));
    assert_eq!(
        validate_value(
            r#"{"oneOf":[{"type":"number"},{"type":"null"}]}"#,
            json_string("42")
        ),
        Ok(parse_value("42"))
    );

    let schema = tool(parse_object(
        r#"{
            "type":"object",
            "properties":{
                "requiredBoolean":{"type":"boolean"},
                "allOfValue":{"allOf":[{"type":"number"}]},
                "object":{
                    "type":"object",
                    "properties":{"count":{"type":"number"}},
                    "additionalProperties":{"type":"boolean"}
                },
                "tuple":{"type":"array","items":[{"type":"number"},{"type":"boolean"}]},
                "list":{"type":"array","items":{"type":"number"}},
                "allOfObject":{"allOf":[{"type":"object","properties":{"value":{"type":"string"}}}]},
                "anyOfObject":{"anyOf":[{"type":"object","properties":{"value":{"type":"string"}}}]},
                "oneOfObject":{"oneOf":[{"type":"object","properties":{"value":{"type":"string"}}}]},
                "additionalOnly":{"type":"object","additionalProperties":{"type":"string"}}
            },
            "required":["requiredBoolean"]
        }"#,
    ));
    let arguments = parse_object(
        r#"{
            "requiredBoolean":null,
            "allOfValue":"3",
            "object":{"count":"4","enabled":"true"},
            "tuple":["5","false"],
            "list":["6","7"],
            "allOfObject":{"value":null},
            "anyOfObject":{"value":null},
            "oneOfObject":{"value":null},
            "additionalOnly":{"value":null}
        }"#,
    );
    let expected = parse_object(
        r#"{
            "requiredBoolean":false,
            "allOfValue":3,
            "object":{"count":4,"enabled":true},
            "tuple":[5,false],
            "list":[6,7],
            "allOfObject":{"value":""},
            "anyOfObject":{"value":""},
            "oneOfObject":{"value":""},
            "additionalOnly":{"value":""}
        }"#,
    );
    assert_eq!(
        validate_tool_arguments(&schema, &call(arguments)),
        Ok(expected)
    );

    assert_eq!(
        validate_value(
            r#"{"type":"array","items":[{"type":"number"}]}"#,
            parse_value(r#"["8","unchanged"]"#)
        ),
        Ok(parse_value(r#"[8,"unchanged"]"#))
    );
    assert_eq!(
        validate_value(
            r#"{"type":"array","items":{"type":"array","items":{"type":"number"}}}"#,
            parse_value(r#"[["9"],["10"]]"#)
        ),
        Ok(parse_value("[[9],[10]]"))
    );
}

#[test]
fn plain_refs_do_not_coerce_and_invalid_subschemas_are_typed() {
    let referenced_number = tool(parse_object(
        r##"{
            "type":"object",
            "properties":{"value":{"$ref":"#/$defs/number"}},
            "required":["value"],
            "$defs":{"number":{"type":"number"}}
        }"##,
    ));
    assert!(matches!(
        validate_tool_arguments(&referenced_number, &call(parse_object(r#"{"value":"42"}"#))),
        Err(ToolValidationError::InvalidArguments(_))
    ));

    let ref_with_type_sibling = tool(parse_object(
        r##"{
            "type":"object",
            "properties":{"value":{"$ref":"#/$defs/accept","type":"number"}},
            "$defs":{"accept":{}}
        }"##,
    ));
    assert_eq!(
        validate_tool_arguments(
            &ref_with_type_sibling,
            &call(parse_object(r#"{"value":"42"}"#))
        ),
        Ok(parse_object(r#"{"value":42}"#))
    );
    assert_eq!(
        validate_tool_arguments(
            &ref_with_type_sibling,
            &call(parse_object(r#"{"value":null}"#))
        ),
        Ok(parse_object(r#"{"value":0}"#))
    );

    let union_with_local_ref = tool(parse_object(
        r##"{
            "type":"object",
            "properties":{"value":{"anyOf":[{"$ref":"#/$defs/number"},{"type":"string"}]}},
            "required":["value"],
            "$defs":{"number":{"type":"number"}}
        }"##,
    ));
    assert_eq!(
        validate_tool_arguments(&union_with_local_ref, &call(parse_object(r#"{"value":7}"#))),
        Ok(parse_object(r#"{"value":"7"}"#))
    );
    let nullable_union_with_local_ref = tool(parse_object(
        r##"{
            "type":"object",
            "properties":{"value":{"anyOf":[{"$ref":"#/$defs/number"},{"type":"null"}]}},
            "required":["value"],
            "$defs":{"number":{"type":"number"}}
        }"##,
    ));
    assert_eq!(
        validate_tool_arguments(
            &nullable_union_with_local_ref,
            &call(parse_object(r#"{"value":null}"#))
        ),
        Ok(parse_object(r#"{"value":null}"#))
    );

    let invalid_subschema = tool(parse_object(
        r#"{"type":"object","properties":{"value":{"type":42}}}"#,
    ));
    assert_eq!(
        validate_tool_arguments(&invalid_subschema, &call(JsonObject::default())),
        Err(ToolValidationError::InvalidSchema)
    );

    let legacy_items_in_2020 = tool(parse_object(
        r#"{
            "$schema":"https://json-schema.org/draft/2020-12/schema",
            "type":"object",
            "properties":{"value":{"type":"array","items":[{"type":"number"}]}}
        }"#,
    ));
    assert_eq!(
        validate_tool_arguments(&legacy_items_in_2020, &call(JsonObject::default())),
        Err(ToolValidationError::InvalidSchema)
    );
}

#[test]
fn issue_paths_follow_pinned_naive_slash_to_dot_formatting() {
    let issue_path = |schema: &str, arguments: &str| {
        let error =
            validate_tool_arguments(&tool(parse_object(schema)), &call(parse_object(arguments)))
                .expect_err("the witness arguments are invalid");
        let ToolValidationError::InvalidArguments(report) = error else {
            panic!("expected invalid arguments");
        };
        report.issues()[0].path().to_owned()
    };

    assert_eq!(issue_path(r#"{"type":"array"}"#, "{}"), "root");
    assert_eq!(
        issue_path(r#"{"type":"object","required":["foo"]}"#, "{}"),
        "foo"
    );
    assert_eq!(
        issue_path(
            r#"{"type":"object","properties":{"outer":{"type":"object","required":["inner"]}}}"#,
            r#"{"outer":{}}"#
        ),
        "outer.inner"
    );
    assert_eq!(
        issue_path(
            r#"{"type":"object","properties":{"a/b":{"type":"string","pattern":"^good$"}}}"#,
            r#"{"a/b":"bad"}"#
        ),
        "a.b"
    );
    assert_eq!(
        issue_path(r#"{"type":"object","required":["a/b"]}"#, "{}"),
        "a/b"
    );
    assert_eq!(
        issue_path(
            r#"{"type":"object","properties":{"/":{"type":"string","pattern":"^good$"}}}"#,
            r#"{"/":"bad"}"#
        ),
        "."
    );
    assert_eq!(
        issue_path(
            r#"{"type":"object","properties":{"/a":{"type":"string","pattern":"^good$"}}}"#,
            r#"{"/a":"bad"}"#
        ),
        ".a"
    );
    assert_eq!(
        issue_path(
            r#"{"type":"object","properties":{"outer":{"type":"object","properties":{"":{"type":"string","pattern":"^good$"}}}}}"#,
            r#"{"outer":{"":"bad"}}"#
        ),
        "outer."
    );
    assert_eq!(
        issue_path(
            r#"{"type":"object","properties":{"items":{"type":"array","items":{"type":"string","pattern":"^good$"}}}}"#,
            r#"{"items":["good","bad"]}"#
        ),
        "items.1"
    );
    assert_eq!(
        issue_path(
            r#"{"type":"object","properties":{"a~b":{"type":"string","pattern":"^good$"}}}"#,
            r#"{"a~b":"bad"}"#
        ),
        "a~b"
    );

    let multibyte = "界".repeat(100);
    let schema = format!(
        r#"{{"type":"object","properties":{{"{multibyte}":{{"type":"string","pattern":"^good$"}}}}}}"#
    );
    let arguments = format!(r#"{{"{multibyte}":"bad"}}"#);
    assert_eq!(issue_path(&schema, &arguments), "界".repeat(85));
}

#[test]
fn absent_root_schema_uses_draft_7() {
    let draft_7_tuple = tool(parse_object(
        r#"{
            "type":"object",
            "properties":{"value":{"type":"array","items":[{"type":"string"}],"additionalItems":false}},
            "required":["value"]
        }"#,
    ));
    let arguments = parse_object(r#"{"value":["ok"]}"#);
    assert_eq!(
        validate_tool_arguments(&draft_7_tuple, &call(arguments.clone())),
        Ok(arguments)
    );
}

#[test]
fn root_schema_autodetects_draft_4() {
    let draft_4 = tool(parse_object(
        r#"{
            "$schema":"http://json-schema.org/draft-04/schema#",
            "type":"object",
            "properties":{"value":{"type":"number","minimum":1,"exclusiveMinimum":true}},
            "required":["value"]
        }"#,
    ));
    let error = validate_tool_arguments(&draft_4, &call(parse_object(r#"{"value":1}"#)))
        .expect_err("Draft 4 boolean exclusiveMinimum rejects the boundary");
    assert!(matches!(error, ToolValidationError::InvalidArguments(_)));
}

#[test]
fn root_schema_autodetects_draft_2020_12() {
    let draft_2020_12 = tool(parse_object(
        r#"{
            "$schema":"https://json-schema.org/draft/2020-12/schema",
            "type":"object",
            "properties":{"value":{"type":"array","prefixItems":[{"type":"string"}],"items":false}},
            "required":["value"]
        }"#,
    ));
    let arguments = parse_object(r#"{"value":["ok"]}"#);
    assert_eq!(
        validate_tool_arguments(&draft_2020_12, &call(arguments.clone())),
        Ok(arguments)
    );
    assert!(matches!(
        validate_tool_arguments(
            &draft_2020_12,
            &call(parse_object(r#"{"value":["ok","extra"]}"#))
        ),
        Err(ToolValidationError::InvalidArguments(_))
    ));
}

fn assert_invalid_value(schema: &str, value: JsonValue) {
    assert!(matches!(
        validate_value(schema, value),
        Err(ToolValidationError::InvalidArguments(_))
    ));
}

#[test]
fn offline_validator_requires_no_code_generation_and_bounds_cache() {
    let local = tool(parse_object(
        r##"{"$ref":"#/$defs/object","$defs":{"object":{"type":"object"}}}"##,
    ));
    assert!(validate_tool_arguments(&local, &call(JsonObject::default())).is_ok());
    let external = tool(parse_object(
        r##"{"$ref":"https://example.invalid/schema"}"##,
    ));
    assert_eq!(
        validate_tool_arguments(&external, &call(JsonObject::default())),
        Err(ToolValidationError::InvalidSchema)
    );
    for index in 0..20 {
        let schema = format!(r#"{{"type":"object","title":"cache-{index}"}}"#);
        assert!(
            validate_tool_arguments(&tool(parse_object(&schema)), &call(JsonObject::default()))
                .is_ok()
        );
    }
}

#[test]
fn plain_number_coerces_finite_numeric_string() {
    assert_eq!(
        validate_value(r#"{"type":"number"}"#, json_string("42")),
        Ok(parse_value("42"))
    );
}

#[test]
fn plain_number_coerces_true_to_one() {
    assert_eq!(
        validate_value(r#"{"type":"number"}"#, JsonValue::Bool(true)),
        Ok(parse_value("1"))
    );
}

#[test]
fn plain_number_coerces_null_to_zero() {
    assert_eq!(
        validate_value(r#"{"type":"number"}"#, JsonValue::Null),
        Ok(parse_value("0"))
    );
}

#[test]
fn plain_integer_coerces_integral_string() {
    assert_eq!(
        validate_value(r#"{"type":"integer"}"#, json_string("42")),
        Ok(parse_value("42"))
    );
}

#[test]
fn plain_boolean_coerces_true_string() {
    assert_eq!(
        validate_value(r#"{"type":"boolean"}"#, json_string("true")),
        Ok(JsonValue::Bool(true))
    );
}

#[test]
fn plain_boolean_coerces_false_string() {
    assert_eq!(
        validate_value(r#"{"type":"boolean"}"#, json_string("false")),
        Ok(JsonValue::Bool(false))
    );
}

#[test]
fn plain_boolean_coerces_one() {
    assert_eq!(
        validate_value(r#"{"type":"boolean"}"#, parse_value("1")),
        Ok(JsonValue::Bool(true))
    );
}

#[test]
fn plain_boolean_coerces_zero() {
    assert_eq!(
        validate_value(r#"{"type":"boolean"}"#, parse_value("0")),
        Ok(JsonValue::Bool(false))
    );
}

#[test]
fn plain_string_coerces_null_to_empty() {
    assert_eq!(
        validate_value(r#"{"type":"string"}"#, JsonValue::Null),
        Ok(json_string(""))
    );
}

#[test]
fn plain_string_coerces_boolean() {
    assert_eq!(
        validate_value(r#"{"type":"string"}"#, JsonValue::Bool(true)),
        Ok(json_string("true"))
    );
}

#[test]
fn plain_null_coerces_empty_string() {
    assert_eq!(
        validate_value(r#"{"type":"null"}"#, json_string("")),
        Ok(JsonValue::Null)
    );
}

#[test]
fn plain_null_coerces_zero() {
    assert_eq!(
        validate_value(r#"{"type":"null"}"#, parse_value("0")),
        Ok(JsonValue::Null)
    );
}

#[test]
fn plain_null_coerces_false() {
    assert_eq!(
        validate_value(r#"{"type":"null"}"#, JsonValue::Bool(false)),
        Ok(JsonValue::Null)
    );
}

#[test]
fn type_union_preserves_existing_matching_arm() {
    assert_eq!(
        validate_value(r#"{"type":["number","string"]}"#, json_string("1")),
        Ok(json_string("1"))
    );
}

#[test]
fn type_union_tries_later_coercible_arm() {
    assert_eq!(
        validate_value(r#"{"type":["boolean","number"]}"#, json_string("1")),
        Ok(parse_value("1"))
    );
}

#[test]
fn optional_null_normalization_recurses() {
    let schema = tool(parse_object(
        r#"{"type":"object","properties":{"nested":{"type":"object","properties":{"value":{"type":"number"}}}}}"#,
    ));
    assert_eq!(
        validate_tool_arguments(&schema, &call(parse_object(r#"{"nested":{"value":null}}"#))),
        Ok(parse_object(r#"{"nested":{}}"#))
    );
}

#[test]
fn local_nullable_reference_preserves_optional_null() {
    let schema = tool(parse_object(
        r##"{"type":"object","properties":{"value":{"$ref":"#/$defs/value"}},"$defs":{"value":{"type":["number","null"]}}}"##,
    ));
    assert_eq!(
        validate_tool_arguments(&schema, &call(parse_object(r#"{"value":null}"#))),
        Ok(parse_object(r#"{"value":null}"#))
    );
}

#[test]
fn any_of_preserves_existing_null_arm() {
    assert_eq!(
        validate_value(
            r#"{"anyOf":[{"type":"number"},{"type":"null"}]}"#,
            JsonValue::Null
        ),
        Ok(JsonValue::Null)
    );
}

#[test]
fn one_of_preserves_existing_null_arm() {
    assert_eq!(
        validate_value(
            r#"{"oneOf":[{"type":"number"},{"type":"null"}]}"#,
            JsonValue::Null
        ),
        Ok(JsonValue::Null)
    );
}

#[test]
fn any_of_coerces_to_first_valid_arm() {
    assert_eq!(
        validate_value(
            r#"{"anyOf":[{"type":"number"},{"type":"null"}]}"#,
            json_string("42")
        ),
        Ok(parse_value("42"))
    );
}

#[test]
fn nullable_array_type_preserves_null() {
    assert_eq!(
        validate_value(
            r#"{"type":["array","null"],"items":{"type":"string"}}"#,
            JsonValue::Null
        ),
        Ok(JsonValue::Null)
    );
}

#[test]
fn boolean_rejects_numeric_one_string() {
    assert_invalid_value(r#"{"type":"boolean"}"#, json_string("1"));
}

#[test]
fn boolean_rejects_numeric_zero_string() {
    assert_invalid_value(r#"{"type":"boolean"}"#, json_string("0"));
}

#[test]
fn null_rejects_null_string() {
    assert_invalid_value(r#"{"type":"null"}"#, json_string("null"));
}

#[test]
fn integer_rejects_fractional_string() {
    assert_invalid_value(r#"{"type":"integer"}"#, json_string("42.1"));
}

#[test]
fn validate_tool_call_selects_known_exact_name() {
    let first = tool(parse_object(r#"{"type":"object","required":["missing"]}"#));
    let second = tool(JsonObject::default());
    let mut matching_call = call(JsonObject::default());
    matching_call.name = ToolName::new("echo").expect("valid name");
    assert!(matches!(
        validate_tool_call(&[first, second], &matching_call),
        Err(ToolValidationError::InvalidArguments(_))
    ));

    let mismatched = validate_tool_arguments(
        &tool(JsonObject::default()),
        &call(parse_object(r#"{"owned":true}"#)),
    )
    .expect("arguments function does not compare names");
    assert_eq!(mismatched, parse_object(r#"{"owned":true}"#));

    let owned = validate_tool_call(&[tool(JsonObject::default())], &{
        let mut tool_call = call(parse_object(r#"{"owned":true}"#));
        tool_call.name = ToolName::new("echo").expect("valid name");
        tool_call
    })
    .expect("matching call validates");
    assert_eq!(owned, parse_object(r#"{"owned":true}"#));

    let source = parse_object(r#"{"owned":true}"#);
    let mut source_call = call(source.clone());
    source_call.name = ToolName::new("echo").expect("valid name");
    let mut returned = validate_tool_call(&[tool(JsonObject::default())], &source_call)
        .expect("matching call validates")
        .into_map();
    returned.insert("later".to_owned(), JsonValue::Bool(true));
    assert_eq!(source_call.arguments, source);
}

#[test]
fn validate_tool_call_rejects_unknown_name() {
    assert_eq!(
        validate_tool_call(&[tool(JsonObject::default())], &call(JsonObject::default())),
        Err(ToolValidationError::ToolNotFound)
    );
}

#[test]
fn nested_required_issue_has_bounded_dotted_path() {
    let schema = tool(parse_object(
        r#"{"type":"object","properties":{"outer":{"type":"object","required":["inner"]}}}"#,
    ));
    let error = validate_tool_arguments(&schema, &call(parse_object(r#"{"outer":{}}"#)))
        .expect_err("inner is required");
    let ToolValidationError::InvalidArguments(report) = error else {
        panic!("expected invalid arguments");
    };
    assert_eq!(report.issues()[0].path(), "outer.inner");
}

#[test]
fn validation_never_mutates_arguments_or_schema() {
    let mut cases = vec![
        (
            tool(parse_object(
                r#"{"type":"object","properties":{"value":{"type":"number"}},"required":["value"]}"#,
            )),
            call(parse_object(r#"{"value":"42"}"#)),
        ),
        (
            tool(parse_object(
                r#"{"type":"object","properties":{"value":{"type":"boolean"}},"required":["value"]}"#,
            )),
            call(parse_object(r#"{"value":"1"}"#)),
        ),
        (
            tool(parse_object(r#"{"type":42}"#)),
            call(JsonObject::default()),
        ),
        (
            tool(parse_object(
                r##"{"$ref":"https://example.invalid/schema"}"##,
            )),
            call(JsonObject::default()),
        ),
    ];
    cases.push((
        tool(JsonObject::default()),
        call(object([(
            "x".to_owned(),
            json_string(&"a".repeat(1024 * 1024)),
        )])),
    ));
    for (tool, tool_call) in cases {
        let before_tool = tool.clone();
        let before_call = tool_call.clone();
        let _ = validate_tool_arguments(&tool, &tool_call);
        assert_eq!(tool, before_tool);
        assert_eq!(tool_call, before_call);
    }

    let tools = vec![tool(JsonObject::default())];
    let unknown_call = call(parse_object(r#"{"unchanged":true}"#));
    let before_tools = tools.clone();
    let before_call = unknown_call.clone();
    assert_eq!(
        validate_tool_call(&tools, &unknown_call),
        Err(ToolValidationError::ToolNotFound)
    );
    assert_eq!(tools, before_tools);
    assert_eq!(unknown_call, before_call);
}

#[test]
fn validation_report_retains_lexical_smallest_sixteen_and_exact_omitted_count() {
    let properties = (0..20)
        .map(|index| {
            (
                format!("p{index:02}"),
                JsonValue::Object(parse_object(r#"{"type":"string","pattern":"^valid$"}"#)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let schema = tool(object([
        ("type".to_owned(), json_string("object")),
        (
            "properties".to_owned(),
            JsonValue::Object(JsonObject::new(properties)),
        ),
    ]));
    let arguments =
        object((0..20).map(|index| (format!("p{index:02}"), json_string("invalid-secret"))));
    let error = validate_tool_arguments(&schema, &call(arguments)).expect_err("patterns fail");
    assert!(!format!("{error:?} {error}").contains("invalid-secret"));
    let ToolValidationError::InvalidArguments(report) = error else {
        panic!("expected invalid arguments");
    };
    assert_eq!(report.issues().len(), 16);
    assert_eq!(report.omitted_issue_count().get(), 4);
    assert_eq!(report.issues()[0].path(), "p00");
    assert_eq!(report.issues()[15].path(), "p15");
}
