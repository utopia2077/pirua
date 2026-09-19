use pi_extension_protocol::jcs::{Error, Limits, admit_schema, canonical_json, canonical_number};
use serde_json::Value;

const VECTORS: &str = include_str!("fixtures/jcs-vectors.json");
const NODE_RESULTS: &str = include_str!("fixtures/jcs-node-results.jsonl");
const ARGUMENTS: &str = include_str!("fixtures/tool.arguments_echo.schema.json");
const PROGRESS: &str = include_str!("fixtures/tool.progress_echo.schema.json");
const RESULT: &str = include_str!("fixtures/tool.result_echo.schema.json");

#[test]
fn all_38_saved_rfc_and_node_vectors_match() {
    let vectors: Vec<Value> = serde_json::from_str(VECTORS).unwrap();
    let reference: Vec<Value> = NODE_RESULTS
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(vectors.len(), 38);
    assert_eq!(vectors.len(), reference.len());
    for (case, expected) in vectors.iter().zip(&reference) {
        let id = case["id"].as_str().unwrap();
        assert_eq!(id, expected["id"], "{id}");
        let result = match case["mode"].as_str().unwrap() {
            "jcs" => canonical_json(
                case["input"].as_str().unwrap().as_bytes(),
                Limits::default(),
            ),
            "schema" => admit_schema(
                case["input"].as_str().unwrap().as_bytes(),
                Limits::default(),
            )
            .map(|schema| schema.canonical().clone()),
            "number_bits" => {
                let bits = u64::from_str_radix(case["bits"].as_str().unwrap(), 16).unwrap();
                canonical_number(f64::from_bits(bits), Limits::default())
            }
            mode => panic!("unknown mode {mode}"),
        };
        match result {
            Ok(actual) => {
                assert_eq!(case["expect"], "ok", "{id}");
                assert_eq!(expected["status"], "ok", "{id}");
                assert_eq!(
                    std::str::from_utf8(actual.bytes()).unwrap(),
                    expected["canonical"],
                    "{id}"
                );
                assert_eq!(actual.sha256_hex(), expected["sha256"], "{id}");
                if let Some(literal) = case["expected"].as_str() {
                    assert_eq!(
                        std::str::from_utf8(actual.bytes()).unwrap(),
                        literal,
                        "{id}"
                    );
                }
            }
            Err(error) => {
                assert_eq!(case["expect"], "reject", "{id}: {error}");
                assert_eq!(expected["status"], "reject", "{id}: {error}");
            }
        }
    }
}

#[test]
fn three_echo_schemas_admit_and_validate_instances() {
    let args = admit_schema(ARGUMENTS.as_bytes(), Limits::default()).unwrap();
    let progress = admit_schema(PROGRESS.as_bytes(), Limits::default()).unwrap();
    let result = admit_schema(RESULT.as_bytes(), Limits::default()).unwrap();
    assert_eq!(
        args.validate_instance(br#"{"text":"hi"}"#, Limits::default()),
        Ok(())
    );
    assert_eq!(
        progress.validate_instance(
            br#"{"content":[{"type":"text","text":"hi"}],"details":{}}"#,
            Limits::default()
        ),
        Ok(())
    );
    assert_eq!(
        result.validate_instance(
            br#"{"content":[{"type":"text","text":"ok"}],"details":{}}"#,
            Limits::default()
        ),
        Ok(())
    );
    for bad in [
        br#"{}"#.as_slice(),
        br#"{"text":2}"#,
        br#"{"text":"hi","extra":true}"#,
        br#"{"text":"hi","text":"again"}"#,
    ] {
        assert!(args.validate_instance(bad, Limits::default()).is_err());
    }
    for bad in [
        br#"{"content":[],"details":{},"usage":{"input":-1}}"#.as_slice(),
        br#"{"content":[{"type":"image","text":"x"}],"details":{}}"#,
        br#"{"content":[],"details":{"extra":1}}"#,
        br#"{"content":[],"details":{},"addedToolNames":[""]}"#,
    ] {
        assert_eq!(
            result.validate_instance(bad, Limits::default()),
            Err(Error::InstanceMismatch)
        );
    }
    assert_eq!(
        result.validate_instance(
            br#"{"content":[],"details":{},"usage":{"input":9007199254740992}}"#,
            Limits::default()
        ),
        Err(Error::InvalidNumber)
    );
    assert_ne!(args.canonical().sha256(), result.canonical().sha256());
}

#[test]
fn admitted_subset_refs_constraints_and_rejections() {
    let schema = admit_schema(
        br##"{"$defs":{"Atom":{"type":"string","minLength":2,"maxLength":3}},"type":"object","properties":{"v":{"$ref":"#/$defs/Atom"},"n":{"type":"integer","minimum":1,"maximum":3}},"required":["v"],"additionalProperties":false,"minProperties":1,"maxProperties":2}"##,
        Limits::default(),
    ).unwrap();
    assert_eq!(
        schema.validate_instance(r#"{"v":"éx","n":2}"#.as_bytes(), Limits::default()),
        Ok(())
    );
    for bad in [
        br#"{"v":"a"}"#.as_slice(),
        br#"{"v":"abcd"}"#,
        br#"{"v":"ab","n":4}"#,
        br#"{"v":"ab","n":1.5}"#,
        br#"{"v":"ab","x":1}"#,
        br#"{}"#,
    ] {
        assert_eq!(
            schema.validate_instance(bad, Limits::default()),
            Err(Error::InstanceMismatch)
        );
    }
    for bad in [
        br#"{"type":"string","pattern":"x"}"#.as_slice(),
        br#"{"type":"string","format":"email"}"#,
        br#"{"anyOf":[{"type":"string"}]}"#,
        br#"{"$ref":"https://example.invalid/x"}"#,
        br##"{"$defs":{"A":{"$ref":"#/$defs/B"},"B":{"$ref":"#/$defs/A"}},"$ref":"#/$defs/A"}"##,
        br##"{"$defs":{"A":{"$ref":"#/$defs/Missing"}},"$ref":"#/$defs/A"}"##,
        br#"{"type":"array","minLength":1,"items":{"type":"null"}}"#,
        br#"{"type":"array","items":{"type":"null"},"minItems":2,"maxItems":1}"#,
        br#"{"type":"integer","minimum":0.5}"#,
        br#"{"type":"object","properties":{"x":{"type":"string"}},"required":["y"]}"#,
        br#"{"type":"string","enum":["x","x"]}"#,
        br#"{"type":"null","const":false}"#,
        br#"{}"#,
        br#"{"enum":["x"]}"#,
        br#"{"const":"x","minLength":1}"#,
        br#"{"properties":{}}"#,
    ] {
        assert!(admit_schema(bad, Limits::default()).is_err(), "{bad:?}");
    }
    let array = admit_schema(
        br#"{"type":"array","items":{"type":"boolean"},"minItems":1,"maxItems":2}"#,
        Limits::default(),
    )
    .unwrap();
    assert_eq!(
        array.validate_instance(br#"[true,false]"#, Limits::default()),
        Ok(())
    );
    assert_eq!(
        array.validate_instance(br#"[]"#, Limits::default()),
        Err(Error::InstanceMismatch)
    );
    assert_eq!(
        array.validate_instance(br#"[true,false,true]"#, Limits::default()),
        Err(Error::InstanceMismatch)
    );
    let enumeration = admit_schema(
        br#"{"type":"object","enum":[{"a":1,"b":2}]}"#,
        Limits::default(),
    )
    .unwrap();
    assert_eq!(
        enumeration.validate_instance(br#"{"b":2,"a":1}"#, Limits::default()),
        Ok(())
    );
}

#[test]
fn tiny_limits_and_single_byte_changes_are_causal() {
    let input = r#"{"z":"é","a":[1]}"#.as_bytes();
    let baseline = canonical_json(input, Limits::default()).unwrap();
    let modified = canonical_json(r#"{"z":"é","a":[2]}"#.as_bytes(), Limits::default()).unwrap();
    assert_ne!(baseline.sha256(), modified.sha256());
    assert_ne!(baseline.bytes(), modified.bytes());
    let short_input = Limits {
        input_bytes: input.len() - 1,
        ..Limits::default()
    };
    assert_eq!(canonical_json(input, short_input), Err(Error::Limit));
    let short_output = Limits {
        output_bytes: baseline.bytes().len() - 1,
        ..Limits::default()
    };
    assert_eq!(canonical_json(input, short_output), Err(Error::Limit));
    for limits in [
        Limits {
            nodes: 3,
            ..Limits::default()
        },
        Limits {
            elements: 2,
            ..Limits::default()
        },
        Limits {
            depth: 1,
            ..Limits::default()
        },
        Limits {
            decoded_bytes: 2,
            ..Limits::default()
        },
    ] {
        assert_eq!(canonical_json(input, limits), Err(Error::Limit));
    }
    let first = admit_schema(br#"{"type":"string","minLength":1}"#, Limits::default()).unwrap();
    let second = admit_schema(br#"{"type":"string","minLength":2}"#, Limits::default()).unwrap();
    let reordered = admit_schema(br#"{"minLength":1,"type":"string"}"#, Limits::default()).unwrap();
    assert_ne!(first.canonical().sha256(), second.canonical().sha256());
    assert_eq!(first.canonical().sha256(), reordered.canonical().sha256());
    assert_eq!(
        first.validate_instance(br#""x""#, Limits::default()),
        Ok(())
    );
    assert_eq!(
        second.validate_instance(br#""x""#, Limits::default()),
        Err(Error::InstanceMismatch)
    );
    assert_eq!(
        first.validate_instance(
            br#""x""#,
            Limits {
                input_bytes: 2,
                ..Limits::default()
            }
        ),
        Err(Error::Limit)
    );
}

#[test]
fn malformed_and_sensitive_input_is_never_echoed() {
    let duplicate =
        canonical_json(br#"{"secret":"token","secret":"again"}"#, Limits::default()).unwrap_err();
    assert_eq!(duplicate, Error::DuplicateKey);
    assert!(!duplicate.to_string().contains("token"));
    assert_eq!(
        canonical_json(br#"{"x":"\ud800"}"#, Limits::default()),
        Err(Error::InvalidJson)
    );
    assert_eq!(
        admit_schema(
            br#"{"type":"integer","maximum":9007199254740992}"#,
            Limits::default()
        )
        .unwrap_err(),
        Error::InvalidNumber
    );
    assert_eq!(
        canonical_number(f64::INFINITY, Limits::default()),
        Err(Error::InvalidNumber)
    );
}

fn shared_dag(depth: usize) -> Vec<u8> {
    let mut defs = serde_json::Map::new();
    for index in (0..depth).rev() {
        let next = format!("#/$defs/D{}", index + 1);
        defs.insert(
            format!("D{index}"),
            serde_json::json!({
                "type": "object",
                "properties": {"x": {"$ref": next}, "y": {"$ref": next}}
            }),
        );
    }
    defs.insert(format!("D{depth}"), serde_json::json!({"type":"null"}));
    serde_json::to_vec(&serde_json::json!({"$defs": defs, "$ref":"#/$defs/D0"})).unwrap()
}

#[test]
fn shared_definition_dag_admits_under_small_schema_work_budget() {
    for depth in [16, 18] {
        let input = shared_dag(depth);
        let limits = Limits {
            schema_work: 10_000,
            ..Limits::default()
        };
        let admitted = admit_schema(&input, limits).unwrap();
        assert_eq!(
            admitted.validate_instance(b"null", Limits::default()),
            Err(Error::InstanceMismatch)
        );
        assert_eq!(
            admit_schema(
                &input,
                Limits {
                    schema_work: 5,
                    ..Limits::default()
                }
            )
            .unwrap_err(),
            Error::Limit
        );
    }
}

#[test]
fn unique_enum_comparisons_obey_schema_work_budget() {
    let values: Vec<_> = (0..12).map(|index| format!("prefix-{index:02}")).collect();
    let input = serde_json::to_vec(&serde_json::json!({"type":"string","enum":values})).unwrap();
    assert!(admit_schema(&input, Limits::default()).is_ok());
    let tiny = Limits {
        schema_work: 50,
        ..Limits::default()
    };
    assert_eq!(admit_schema(&input, tiny).unwrap_err(), Error::Limit);
}

#[test]
fn review_size_unique_enum_rejects_with_default_work_budget() {
    let values: Vec<_> = (0..16_000)
        .map(|index| format!("{}{:05}", "x".repeat(30), index))
        .collect();
    let input = serde_json::to_vec(&serde_json::json!({"type":"string","enum":values})).unwrap();
    assert!(input.len() < Limits::default().input_bytes);
    assert_eq!(
        admit_schema(&input, Limits::default()).unwrap_err(),
        Error::Limit
    );
}

fn object_const_pair(keys: usize) -> (String, String) {
    let entry = |index| format!("\"k{index:04}\":{index}");
    let ascending = (0..keys).map(entry).collect::<Vec<_>>().join(",");
    let descending = (0..keys).rev().map(entry).collect::<Vec<_>>().join(",");
    (
        format!("{{\"type\":\"object\",\"const\":{{{ascending}}}}}"),
        format!("{{{descending}}}"),
    )
}

#[test]
fn reversed_const_object_comparison_obeys_instance_work_budget() {
    let (schema, instance) = object_const_pair(512);
    let admitted = admit_schema(schema.as_bytes(), Limits::default()).unwrap();
    let tiny = Limits {
        instance_work: 50,
        ..Limits::default()
    };
    assert!(matches!(
        admitted.validate_instance(instance.as_bytes(), tiny),
        Err(Error::Limit)
    ));
    let (small_schema, small_instance) = object_const_pair(20);
    let small = admit_schema(small_schema.as_bytes(), Limits::default()).unwrap();
    assert_eq!(
        small.validate_instance(small_instance.as_bytes(), Limits::default()),
        Ok(())
    );
}

#[test]
fn review_size_reversed_const_object_rejects_with_default_work_budget() {
    let (schema, instance) = object_const_pair(8_000);
    assert!(schema.len() < Limits::default().input_bytes);
    assert!(instance.len() < Limits::default().input_bytes);
    let admitted = admit_schema(schema.as_bytes(), Limits::default()).unwrap();
    assert_eq!(
        admitted.validate_instance(instance.as_bytes(), Limits::default()),
        Err(Error::Limit)
    );
}

#[test]
fn required_and_property_lookups_obey_instance_work_budget() {
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();
    for index in 0..128 {
        let name = format!("k{index:04}");
        properties.insert(name.clone(), serde_json::json!({"type":"integer"}));
        required.push(name);
    }
    let schema = serde_json::to_vec(
        &serde_json::json!({"type":"object","properties":properties,"required":required}),
    )
    .unwrap();
    let (_, instance) = object_const_pair(128);
    let admitted = admit_schema(&schema, Limits::default()).unwrap();
    assert_eq!(
        admitted.validate_instance(instance.as_bytes(), Limits::default()),
        Ok(())
    );
    assert_eq!(
        admitted.validate_instance(
            instance.as_bytes(),
            Limits {
                instance_work: 50,
                ..Limits::default()
            }
        ),
        Err(Error::Limit)
    );
}
