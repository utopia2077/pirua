#![cfg(feature = "extensions")]

use std::collections::BTreeMap;

use pi_ai_core::{
    FiniteF64, JS_SAFE_INTEGER_MAX, JsonNumber, JsonObject, JsonValue, SafeSigned, SafeUnsigned,
};
use pi_extension_protocol::activation::{
    Activation, AgentTool, AgentToolMode, ApprovedTool, HostAuthority, ToolReplay, ToolSchemaInputs,
};
use pi_extension_protocol::jcs::{self, Limits as JsonLimits};
use pi_extension_protocol::tool_live::SelectedArguments;
use pi_extension_protocol::wire::{v1_0, v1_1};
use pi_harness_runtime::extensions::value::{EncodeError, encode_arguments};
use serde_json::Value;

const CATALOG: &str = include_str!("../../pi-extension-protocol/schema/fixtures/host-catalog.json");
const VECTORS: &str =
    include_str!("../../pi-extension-protocol/schema/fixtures/activation-vectors.json");
const ECHO_SCHEMA: &str =
    include_str!("../../pi-extension-protocol/schema/dynamic/tool.arguments_echo.schema.json");

fn hex(value: &str) -> Vec<u8> {
    let (pairs, tail) = value.as_bytes().as_chunks::<2>();
    assert!(tail.is_empty());
    pairs
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
fn limits() -> v1_1::ProtocolLimits {
    v1_1::ProtocolLimits {
        pre_handshake_frame_bytes: 4096,
        frame_bytes: 8192,
        aggregate_decoded_bytes: 8192,
        value_depth: 32,
        value_elements: 1024,
        string_bytes: 4096,
        bytes_length: 4096,
        in_flight_requests: 8,
        blocking_queue_bytes: 4096,
        progress_queue_bytes: 4096,
        passive_queue_bytes: 4096,
        telemetry_queue_bytes: 4096,
        nesting_depth: 8,
        requests_per_second: 32,
        handshake_ms: 1000,
        cancellation_grace_ms: 100,
        max_stream_credit_bytes: 4096,
    }
}
fn hello() -> Vec<u8> {
    let mut bytes = vec![0x0a, 0x04, 0x08, 0x01, 0x18, 0x01, 0x12, 0x0d, 0x0a, 0x07];
    bytes.extend_from_slice(b"tool.v1");
    bytes.extend_from_slice(&[0x10, 0x01, 0x20, 0x01]);
    bytes
}
fn active(schema: &[u8], selected_minor: u32, host_limits: v1_1::ProtocolLimits) -> Activation {
    let catalog: Value = serde_json::from_str(CATALOG).unwrap();
    let entry = &catalog["entries"][0];
    let parameters: Value = serde_json::from_slice(schema).unwrap();
    let descriptor = AgentTool {
        name: "echo".into(),
        label: "Echo".into(),
        description: "Return supplied content.".into(),
        parameters,
        execution_mode: AgentToolMode::Sequential,
        replay: ToolReplay::Safe,
    };
    let inputs = [0, 1]
        .into_iter()
        .map(|minor| {
            let set = &entry["selected_minor_schema_sets"][format!("1.{minor}")];
            ToolSchemaInputs {
                minor,
                arguments: schema.to_vec(),
                progress: serde_json::to_vec(&set["progress"]["schema"]).unwrap(),
                raw_result: serde_json::to_vec(&set["raw_result"]["schema"]).unwrap(),
            }
        })
        .collect();
    let tool =
        ApprovedTool::new("peer-a".into(), descriptor, inputs, JsonLimits::default()).unwrap();
    let host = HostAuthority::new(
        "peer-a".into(),
        7,
        selected_minor,
        selected_minor,
        host_limits,
        vec![tool],
        vec![],
    )
    .unwrap();
    let mut activation = Activation::new(host, 100);
    activation.receive_hello(&hello(), 101).unwrap();
    let fixture: Value = serde_json::from_str(VECTORS).unwrap();
    let mut manifest = hex(fixture["cases"]["valid_1_1"]["manifest_pb_hex"]
        .as_str()
        .unwrap());
    let old_digest = hex(
        entry["selected_minor_schema_sets"]["1.0"]["arguments"]["schema_digest_sha256"]
            .as_str()
            .unwrap(),
    );
    let new_digest = jcs::admit_schema(schema, JsonLimits::default())
        .unwrap()
        .canonical()
        .sha256();
    let mut replaced = 0;
    for offset in 0..=manifest.len() - 32 {
        if manifest[offset..offset + 32] == old_digest {
            manifest[offset..offset + 32].copy_from_slice(&new_digest);
            replaced += 1;
        }
    }
    assert_eq!(replaced, 2);
    activation.receive_manifest(&manifest, 102).unwrap();
    activation.receive_ready(&[], 103).unwrap();
    activation
}
fn echo() -> JsonObject {
    serde_json::from_str(r#"{"text":"ping"}"#).unwrap()
}
fn bound_map_0(bound: &v1_0::SchemaBoundValue) -> &[v1_0::WireMapEntry] {
    let Some(v1_0::wire_value::Value::MapValue(map)) = bound.value.as_ref().unwrap().value.as_ref()
    else {
        panic!()
    };
    &map.entries
}
fn bound_map_1(bound: &v1_1::SchemaBoundValue) -> &[v1_1::WireMapEntry] {
    let Some(v1_1::wire_value::Value::MapValue(map)) = bound.value.as_ref().unwrap().value.as_ref()
    else {
        panic!()
    };
    &map.entries
}

#[test]
fn echo_arguments_use_selected_minor_and_host_projection() {
    for minor in [0, 1] {
        let a = active(ECHO_SCHEMA.as_bytes(), minor, limits());
        let grant = a.active_grant().unwrap();
        let selected = encode_arguments(&echo(), grant, "echo").unwrap();
        let expected = grant.tools()[0].projections().arguments();
        match (minor, selected) {
            (0, SelectedArguments::V1_0(bound)) => {
                assert_eq!(bound.projection_id, "tool.arguments:echo");
                assert_eq!(bound.schema_digest_sha256, expected.digest());
                let entries = bound_map_0(&bound);
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].key, "text");
                assert!(
                    matches!(entries[0].value.as_ref().unwrap().value,Some(v1_0::wire_value::Value::TextValue(ref text)) if text=="ping")
                );
            }
            (1, SelectedArguments::V1_1(bound)) => {
                assert_eq!(bound.projection_id, "tool.arguments:echo");
                assert_eq!(bound.schema_digest_sha256, expected.digest());
                let entries = bound_map_1(&bound);
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].key, "text");
                assert!(
                    matches!(entries[0].value.as_ref().unwrap().value,Some(v1_1::wire_value::Value::TextValue(ref text)) if text=="ping")
                );
            }
            _ => panic!("wrong selected minor"),
        }
        assert_eq!(
            encode_arguments(&echo(), grant, "other").err(),
            Some(EncodeError::InvalidGrant)
        );
    }
}

const SHAPES_SCHEMA: &str = r#"{"type":"object","additionalProperties":false,"properties":{"arr":{"type":"array","items":{"type":"number"}},"nil":{"type":"null"},"obj":{"type":"object","additionalProperties":false,"maxProperties":0}},"required":["arr","nil","obj"]}"#;

#[test]
fn explicit_null_and_empty_containers_keep_oneof_presence_and_sorted_keys() {
    let saved: JsonObject = serde_json::from_str(r#"{"obj":{},"nil":null,"arr":[]}"#).unwrap();
    for minor in [0, 1] {
        let a = active(SHAPES_SCHEMA.as_bytes(), minor, limits());
        let bound = encode_arguments(&saved, a.active_grant().unwrap(), "echo").unwrap();
        match bound {
            SelectedArguments::V1_0(bound) => {
                let entries = bound_map_0(&bound);
                assert_eq!(
                    entries.iter().map(|e| e.key.as_str()).collect::<Vec<_>>(),
                    ["arr", "nil", "obj"]
                );
                assert!(
                    matches!(entries[0].value.as_ref().unwrap().value,Some(v1_0::wire_value::Value::ListValue(ref list)) if list.items.is_empty())
                );
                assert!(matches!(
                    entries[1].value.as_ref().unwrap().value,
                    Some(v1_0::wire_value::Value::NullValue(_))
                ));
                assert!(
                    matches!(entries[2].value.as_ref().unwrap().value,Some(v1_0::wire_value::Value::MapValue(ref map)) if map.entries.is_empty())
                );
            }
            SelectedArguments::V1_1(bound) => {
                let entries = bound_map_1(&bound);
                assert_eq!(
                    entries.iter().map(|e| e.key.as_str()).collect::<Vec<_>>(),
                    ["arr", "nil", "obj"]
                );
                assert!(
                    matches!(entries[0].value.as_ref().unwrap().value,Some(v1_1::wire_value::Value::ListValue(ref list)) if list.items.is_empty())
                );
                assert!(matches!(
                    entries[1].value.as_ref().unwrap().value,
                    Some(v1_1::wire_value::Value::NullValue(_))
                ));
                assert!(
                    matches!(entries[2].value.as_ref().unwrap().value,Some(v1_1::wire_value::Value::MapValue(ref map)) if map.entries.is_empty())
                );
            }
        }
    }
}

const NUMBERS_SCHEMA: &str = r#"{"type":"object","additionalProperties":false,"properties":{"float":{"type":"number"},"negative":{"type":"integer"},"positive":{"type":"integer"}},"required":["float","negative","positive"]}"#;

#[test]
fn integer_and_float_arms_preserve_safe_numeric_kinds() {
    let saved = JsonObject::new(BTreeMap::from([
        (
            "float".into(),
            JsonValue::Number(JsonNumber::Float(FiniteF64::new(1.0).unwrap())),
        ),
        (
            "negative".into(),
            JsonValue::Number(JsonNumber::Signed(
                SafeSigned::new(-(JS_SAFE_INTEGER_MAX as i64)).unwrap(),
            )),
        ),
        (
            "positive".into(),
            JsonValue::Number(JsonNumber::Unsigned(SafeUnsigned::MAX)),
        ),
    ]));
    for minor in [0, 1] {
        let a = active(NUMBERS_SCHEMA.as_bytes(), minor, limits());
        let bound = encode_arguments(&saved, a.active_grant().unwrap(), "echo").unwrap();
        match bound {
            SelectedArguments::V1_0(bound) => {
                let entries = bound_map_0(&bound);
                assert!(
                    matches!(entries[0].value.as_ref().unwrap().value,Some(v1_0::wire_value::Value::FloatValue(v)) if v==1.0)
                );
                assert!(
                    matches!(entries[1].value.as_ref().unwrap().value,Some(v1_0::wire_value::Value::IntegerValue(v)) if v==-(JS_SAFE_INTEGER_MAX as i64))
                );
                assert!(
                    matches!(entries[2].value.as_ref().unwrap().value,Some(v1_0::wire_value::Value::IntegerValue(v)) if v==JS_SAFE_INTEGER_MAX as i64)
                );
            }
            SelectedArguments::V1_1(bound) => {
                let entries = bound_map_1(&bound);
                assert!(
                    matches!(entries[0].value.as_ref().unwrap().value,Some(v1_1::wire_value::Value::FloatValue(v)) if v==1.0)
                );
                assert!(
                    matches!(entries[1].value.as_ref().unwrap().value,Some(v1_1::wire_value::Value::IntegerValue(v)) if v==-(JS_SAFE_INTEGER_MAX as i64))
                );
                assert!(
                    matches!(entries[2].value.as_ref().unwrap().value,Some(v1_1::wire_value::Value::IntegerValue(v)) if v==JS_SAFE_INTEGER_MAX as i64)
                );
            }
        }
    }
}

#[test]
fn tiny_host_budgets_reject_before_copy_and_errors_hide_values() {
    let mut depth = limits();
    depth.value_depth = 1;
    let a = active(ECHO_SCHEMA.as_bytes(), 1, depth);
    assert_eq!(
        encode_arguments(&echo(), a.active_grant().unwrap(), "echo").err(),
        Some(EncodeError::Limit)
    );
    let mut elements = limits();
    elements.value_elements = 1;
    let saved: JsonObject =
        serde_json::from_str(r#"{"float":1.0,"negative":-1,"positive":1}"#).unwrap();
    let a = active(NUMBERS_SCHEMA.as_bytes(), 1, elements);
    assert_eq!(
        encode_arguments(&saved, a.active_grant().unwrap(), "echo").err(),
        Some(EncodeError::Limit)
    );
    let mut strings = limits();
    strings.string_bytes = 24;
    let a = active(ECHO_SCHEMA.as_bytes(), 1, strings);
    let marker = "SECRET_SENTINEL".repeat(4);
    let saved = JsonObject::new(BTreeMap::from([(
        "text".into(),
        JsonValue::String(marker.clone()),
    )]));
    let error = encode_arguments(&saved, a.active_grant().unwrap(), "echo")
        .err()
        .unwrap();
    assert_eq!(error, EncodeError::Limit);
    assert!(!error.to_string().contains(&marker));
    assert!(!format!("{error:?}").contains(&marker));
    let mut decoded = limits();
    decoded.aggregate_decoded_bytes = 16;
    let a = active(ECHO_SCHEMA.as_bytes(), 1, decoded);
    assert_eq!(
        encode_arguments(&echo(), a.active_grant().unwrap(), "echo").err(),
        Some(EncodeError::Limit)
    );
    let mut frame = limits();
    frame.pre_handshake_frame_bytes = 512;
    frame.frame_bytes = 512;
    let a = active(ECHO_SCHEMA.as_bytes(), 1, frame);
    let long = JsonObject::new(BTreeMap::from([(
        "text".into(),
        JsonValue::String("x".repeat(500)),
    )]));
    assert_eq!(
        encode_arguments(&long, a.active_grant().unwrap(), "echo").err(),
        Some(EncodeError::Limit)
    );
}

#[test]
fn malformed_local_json_is_rejected_by_its_closed_constructors() {
    assert!(serde_json::from_str::<JsonObject>(r#"{"x":1,"x":2}"#).is_err());
    assert!(SafeUnsigned::new(JS_SAFE_INTEGER_MAX + 1).is_err());
    assert!(SafeSigned::new(-((JS_SAFE_INTEGER_MAX + 1) as i64)).is_err());
    assert!(FiniteF64::new(f64::INFINITY).is_err());
}

#[test]
fn selected_schema_rejects_wrong_saved_argument_type() {
    let a = active(ECHO_SCHEMA.as_bytes(), 1, limits());
    let wrong = JsonObject::new(BTreeMap::from([("text".into(), JsonValue::Bool(true))]));
    let error = encode_arguments(&wrong, a.active_grant().unwrap(), "echo")
        .err()
        .unwrap();
    assert!(matches!(error, EncodeError::Value(_)));
    assert_eq!(
        error.to_string(),
        "saved arguments do not match approved schema"
    );
}
